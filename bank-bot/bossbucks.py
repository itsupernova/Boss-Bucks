import discord
from discord.ext import commands
import aiohttp
import asyncio
import sqlite3
import math
import re
from datetime import datetime
import sys 

BOT_PREFIX = '!'
ADMIN_ROLE_NAME = 'Showdown Manager'  
DB_PATH = 'bank.db' 
SHOWDOWN_BANK_START = 40_000 
SHOWDOWN_FUND_ID = 1 
INTENTS = discord.Intents.default()
INTENTS.message_content = True 

bot = commands.Bot(command_prefix=BOT_PREFIX, intents=INTENTS)

def db_connect():
    """
    Establishes and returns a connection to the SQLite database.
    """
    return sqlite3.connect(DB_PATH)

def init_db():
    """
    Initializes all necessary database tables for both the main bank and Showdown features.
    This function will create tables if they do not already exist.
    """
    conn = db_connect()
    c = conn.cursor()

    c.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id TEXT PRIMARY KEY,
            balance REAL DEFAULT 0,
            loan REAL DEFAULT 0,
            loan_last_updated TEXT
        )
    ''')

    c.execute('''
        CREATE TABLE IF NOT EXISTS bank (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            reserve REAL
        )
    ''')

    c.execute('SELECT reserve FROM bank WHERE id = 1')
    if c.fetchone() is None:
        c.execute('INSERT INTO bank (id, reserve) VALUES (1, ?)', (1_000_000,))

    c.execute('''
        CREATE TABLE IF NOT EXISTS showdown_players (
            discord_id TEXT PRIMARY KEY,
            showdown_username TEXT UNIQUE,
            showdown_username_lower TEXT UNIQUE,
            matches_played INTEGER DEFAULT 0,
            wins INTEGER DEFAULT 0,
            losses INTEGER DEFAULT 0
        )
    ''')

    c.execute('''
        CREATE TABLE IF NOT EXISTS showdown_bank (
            id INTEGER PRIMARY KEY CHECK(id = 1),
            reserve REAL
        )
    ''')

    c.execute('SELECT reserve FROM showdown_bank WHERE id = 1')
    row = c.fetchone()
    if row is None:
        c.execute('INSERT INTO showdown_bank (id, reserve) VALUES (1, ?)', (SHOWDOWN_BANK_START,))

    c.execute('''
        CREATE TABLE IF NOT EXISTS matches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            winner_discord_id TEXT,
            loser_discord_id TEXT,
            winner_matches_after INTEGER,
            loser_matches_after INTEGER,
            win_amount INTEGER,
            lose_amount_requested INTEGER,
            lose_amount_taken INTEGER,
            replay_url TEXT UNIQUE,
            timestamp TEXT
        )
    ''')

    c.execute('''
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT,
            type TEXT,
            amount REAL,
            target_user_id TEXT,
            timestamp TEXT
        )
    ''')

    conn.commit()
    conn.close()

def check_db_integrity():
    """
    Performs a basic integrity check on the database to ensure all required tables
    and columns are present for both bank and showdown functionalities.
    Exits the program if critical components are missing.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()

        c.execute("PRAGMA table_info(bank);")
        columns = [col[1] for col in c.fetchall()]
        if 'reserve' not in columns:
            print("❌ ERROR: 'bank' table is missing required columns ('reserve').")
            sys.exit(1)

        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='users';")
        if not c.fetchone():
            print("❌ ERROR: 'users' table is missing.")
            sys.exit(1)

        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='transactions';")
        if not c.fetchone():
            print("❌ ERROR: 'transactions' table is missing.")
            sys.exit(1)

        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='showdown_players';")
        if not c.fetchone():
            print("❌ ERROR: 'showdown_players' table is missing.")
            sys.exit(1)

        c.execute("PRAGMA table_info(showdown_bank);")
        columns = [col[1] for col in c.fetchall()]
        if 'reserve' not in columns:
            print("❌ ERROR: 'showdown_bank' table is missing required columns ('reserve').")
            sys.exit(1)

        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='matches';")
        if not c.fetchone():
            print("❌ ERROR: 'matches' table is missing.")
            sys.exit(1)

        c.execute("PRAGMA index_list(matches);")
        indexes = c.fetchall()
        has_unique_replay_url = False
        for index in indexes:
            index_name = index[1]
            c.execute(f"PRAGMA index_info({index_name});")
            index_columns = c.fetchall()
            for col_info in index_columns:
                if col_info[2] == 'replay_url' and index[2] == 1:
                    has_unique_replay_url = True
                    break
            if has_unique_replay_url:
                    break

        if not has_unique_replay_url:
            print("⚠️ WARNING: 'matches' table does not have a UNIQUE constraint on 'replay_url'. Duplicate replays might be accepted.")


        conn.close()
        print("✅ Database integrity check passed.")
    except Exception as e:
        print(f"❌ Database check failed: {e}")
        sys.exit(1)


def log_transaction(user_id: str, type_: str, amount: float, target_user_id: str | None = None):
    """
    Logs a financial transaction into the 'transactions' table.
    """
    conn = db_connect()
    c = conn.cursor()
    c.execute('''INSERT INTO transactions (user_id, type, amount, target_user_id, timestamp)
                  VALUES (?, ?, ?, ?, ?)''', (user_id, type_, amount, target_user_id, datetime.utcnow().isoformat()))
    conn.commit()
    conn.close()

def apply_interest(loan, last_updated_str):
    """
    Applies interest to a loan based on the time passed since the last update.
    Interest is applied every 3 days at an 8% rate.
    """
    if not last_updated_str:
        return loan, datetime.utcnow().isoformat()

    now = datetime.utcnow()
    last_updated = datetime.fromisoformat(last_updated_str)
    days_passed = (now - last_updated).days
    intervals = days_passed // 3

    if intervals > 0:
        loan *= (1.08) ** intervals
        last_updated = now

    return loan, last_updated.isoformat()

def get_bank_user(discord_id: int):
    """
    Retrieves a user's balance, loan, and loan last updated timestamp from the main 'users' table.
    """
    conn = db_connect()
    c = conn.cursor()
    c.execute('SELECT balance, loan, loan_last_updated FROM users WHERE user_id = ?', (str(discord_id),))
    row = c.fetchone()
    conn.close()
    return row

def update_bank_user(user_id, balance=None, loan=None, loan_last_updated=None):
    """
    Updates a user's balance, loan, or loan last updated timestamp in the database.
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    if balance is not None:
        c.execute('UPDATE users SET balance = ? WHERE user_id = ?', (balance, str(user_id)))
    if loan is not None:
        c.execute('UPDATE users SET loan = ? WHERE user_id = ?', (loan, str(user_id)))
    if loan_last_updated is not None:
        c.execute('UPDATE users SET loan_last_updated = ? WHERE user_id = ?', (loan_last_updated, str(user_id)))
    conn.commit()
    conn.close()

def update_bank_user_balance(discord_id: int, new_balance: float):
    """
    Updates a user's balance in the main 'users' table.
    """
    conn = db_connect()
    c = conn.cursor()
    c.execute('UPDATE users SET balance = ? WHERE user_id = ?', (new_balance, str(discord_id)))
    conn.commit()
    conn.close()

def get_main_bank_reserve():
    """
    Retrieves the current main bank reserve from the database.
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT reserve FROM bank WHERE id = 1')
    reserve = c.fetchone()[0]
    conn.close()
    return reserve

def update_main_bank_reserve(amount):
    """
    Updates the main bank reserve in the database.
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('UPDATE bank SET reserve = ? WHERE id = 1', (amount,))
    conn.commit()
    conn.close()

def is_admin(ctx):
    """
    Checks if the command invoker has administrator permissions in the guild.
    """
    return ctx.author.guild_permissions.administrator


def get_showdown_player(discord_id: int):
    """
    Retrieves a player's Showdown data from the 'showdown_players' table.
    """
    conn = db_connect()
    c = conn.cursor()
    c.execute('SELECT discord_id, showdown_username, showdown_username_lower, matches_played, wins, losses FROM showdown_players WHERE discord_id = ?', (str(discord_id),))
    row = c.fetchone()
    conn.close()
    return row

def get_showdown_player_by_username(username: str):
    """
    Retrieves a player's Showdown data by their Showdown username (case-insensitive).
    """
    username_lower = username.lower()
    conn = db_connect()
    c = conn.cursor()
    c.execute('SELECT discord_id, showdown_username, matches_played, wins, losses FROM showdown_players WHERE showdown_username_lower = ?', (username_lower,))
    row = c.fetchone()
    conn.close()
    return row

def register_showdown_player(discord_id: int, username: str) -> bool:
    """
    Registers a new player in the 'showdown_players' table.
    """
    username_lower = username.lower()
    conn = db_connect()
    c = conn.cursor()
    try:
        c.execute('INSERT INTO showdown_players (discord_id, showdown_username, showdown_username_lower) VALUES (?, ?, ?)', (str(discord_id), username, username_lower))
        conn.commit()
        success = True
    except sqlite3.IntegrityError:
        success = False
    conn.close()
    return success

def update_showdown_stats(winner_id: int, loser_id: int, win_amount: int, lose_requested: int, lose_taken: int, matches_after_w: int, matches_after_l: int, replay_url: str):
    """
    Updates the match statistics for winner and loser, and logs the match details.
    """
    conn = db_connect()
    c = conn.cursor()
    c.execute('UPDATE showdown_players SET matches_played = ?, wins = wins + 1 WHERE discord_id = ?', (matches_after_w, str(winner_id)))
    c.execute('UPDATE showdown_players SET matches_played = ?, losses = losses + 1 WHERE discord_id = ?', (matches_after_l, str(loser_id)))
    c.execute('''INSERT INTO matches (winner_discord_id, loser_discord_id, winner_matches_after, loser_matches_after, win_amount, lose_amount_requested, lose_amount_taken, replay_url, timestamp)
                  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''', (str(winner_id), str(loser_id), matches_after_w, matches_after_l, win_amount, lose_requested, lose_taken, replay_url, datetime.utcnow().isoformat()))
    conn.commit()
    conn.close()

def is_replay_already_submitted(replay_url: str) -> bool:
    """
    Checks if a given replay URL already exists in the 'matches' table.
    """
    conn = db_connect()
    c = conn.cursor()
    c.execute('SELECT 1 FROM matches WHERE replay_url = ?', (replay_url,))
    exists = c.fetchone() is not None
    conn.close()
    return exists

def reset_showdown_stats(discord_id: str):
    """
    Resets a player's Showdown match history (matches_played, wins, losses) to 0.
    """
    conn = db_connect()
    c = conn.cursor()
    c.execute('UPDATE showdown_players SET matches_played = 0, wins = 0, losses = 0 WHERE discord_id = ?', (discord_id,))
    conn.commit()
    conn.close()

def get_showdown_fund() -> float:
    """
    Retrieves the current balance of the Showdown fund.
    """
    conn = db_connect()
    c = conn.cursor()
    c.execute('SELECT reserve FROM showdown_bank WHERE id = 1')
    row = c.fetchone()
    conn.close()
    return row[0] if row else 0.0

def update_showdown_fund(new_amount: float):
    """
    Updates the balance of the Showdown fund.
    """
    conn = db_connect()
    c = conn.cursor()
    c.execute('UPDATE showdown_bank SET reserve = ? WHERE id = 1', (new_amount,))
    conn.commit()
    conn.close()

def has_admin_role(member: discord.Member) -> bool:
    """
    Checks if a Discord member has the configured admin role.
    """
    return any(r.name == ADMIN_ROLE_NAME for r in member.roles)

WINNER_PATTERN_JSON = re.compile(r'"winner"\s*:\s*"([^"\\]+)"')
PLAYER_PATTERN = re.compile(r'\|player\|p[12]\|([^|]+)\|')
WIN_LINE_PATTERN = re.compile(r'\|win\|([^|]+)')

async def fetch_text(session: aiohttp.ClientSession, url: str) -> str | None:
    """
    Fetches text content from a given URL asynchronously.
    """
    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with session.get(url, timeout=timeout) as resp:
            if resp.status == 200:
                return await resp.text()
    except Exception:
        return None
    return None

async def parse_replay(session: aiohttp.ClientSession, base_url: str):
    """
    Parses a Showdown replay URL to extract the winner and all participating players.
    Uses a fuzzy matching algorithm to find the closest player name to the winner.
    """
    base_url = base_url.strip()
    if base_url.endswith('/'):
        base_url = base_url[:-1]
    if not base_url.startswith('http'):
        base_url = 'https://' + base_url

    if '.json' in base_url:
        json_url = base_url
    else:
        json_url = base_url + '.json'

    text = await fetch_text(session, json_url)
    if not text:
        text = await fetch_text(session, base_url)
        if not text:
            raise ValueError('Could not fetch replay data. Please check the URL.')

    raw_winner_from_json_field = None
    m = WINNER_PATTERN_JSON.search(text)
    if m:
        raw_winner_from_json_field = m.group(1)

    if not raw_winner_from_json_field:
        m2 = WIN_LINE_PATTERN.search(text)
        if m2:
            raw_winner_from_json_field = m2.group(1)

    if not raw_winner_from_json_field:
        raise ValueError('Winner not found in replay. Ensure the replay is valid and complete.')
    
    cleaned_winner_name = re.sub(r'\s+', '', raw_winner_from_json_field).strip()

    clean_player_names_from_log = set()
    for p_match in PLAYER_PATTERN.findall(text):
        cleaned_name = re.sub(r'\s+', '', p_match).strip()
        clean_player_names_from_log.add(cleaned_name)

    definitive_winner_name = None
    for player_name in clean_player_names_from_log:
        if cleaned_winner_name.lower() == player_name.lower():
            definitive_winner_name = player_name
            break

    if definitive_winner_name is None:
        best_score = 0
        best_match_name = None

        winner_name_lower = cleaned_winner_name.lower()
        
        for player_name in clean_player_names_from_log:
            player_name_lower = player_name.lower()
            
            score = len(set(winner_name_lower) & set(player_name_lower))
            
            if score > best_score:
                best_score = score
                best_match_name = player_name

        threshold = len(winner_name_lower) / 2
        if best_score >= threshold and best_match_name is not None:
            definitive_winner_name = best_match_name
    
    if definitive_winner_name is None:
        raise ValueError(f'Could not resolve winner "{raw_winner_from_json_field}" to a registered player name from replay participants.')

    return definitive_winner_name, list(clean_player_names_from_log)


def compute_rewards(winner_matches_prev: int, loser_matches_prev: int):
    """
    Calculates the coin rewards for the winner and penalties for the loser
    based on their previous match counts.
    """
    W = winner_matches_prev + 1
    L = loser_matches_prev + 1
    win_amount = 729 // W
    lose_raw = (math.pi ** L) / 2 + 35
    lose_capped = min(lose_raw, 6000)
    lose_amount_requested = int(lose_capped)
    return win_amount, lose_amount_requested, W, L

@bot.event
async def on_ready():
    """
    Event handler that is called when the bot successfully connects to Discord.
    """
    print(f'✅ Bot online as {bot.user}')

@bot.event
async def on_message(message):
    """
    Custom on_message event handler to process specific keyword mentions.
    Ensures that commands are still processed after custom logic.
    """
    if message.author.bot:
        return

    content = message.content.lower()

    if bot.user in message.mentions:
        if "say it maria" in content:
            await message.channel.send("master nova is the best ❤️❤️❤️")
        elif "welcome maria" in content:
            await message.channel.send("thanks master nova 🩷🥺✨")

    await bot.process_commands(message)

@bot.command()
async def register(ctx):
    """
    Registers a new user in the bank system.
    """
    user_id = str(ctx.author.id)
    if get_bank_user(ctx.author.id):
        await ctx.send("❗ You are already registered with the bank.")
        return

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('INSERT INTO users (user_id, balance, loan, loan_last_updated) VALUES (?, ?, ?, ?)',
              (user_id, 0, 0, datetime.utcnow().isoformat()))
    conn.commit()
    conn.close()
    await ctx.send("✅ Bank registration successful!")

@bot.command()
async def balance(ctx):
    """
    Displays the user's current balance and loan amount.
    """
    user_id = str(ctx.author.id)
    data = get_bank_user(ctx.author.id)
    if not data:
        await ctx.send("❗ You need to `!register` first.")
        return

    balance, loan, last_updated = data
    loan, new_time = apply_interest(loan, last_updated)
    update_bank_user(user_id, loan=loan, loan_last_updated=new_time)

    await ctx.send(f"💰 Balance: {balance:.2f} coins\n💳 Loan: {loan:.2f} coins")

@bot.command()
async def loan(ctx, amount: float):
    """
    Allows a user to take a loan from the bank.
    """
    user_id = str(ctx.author.id)
    if amount <= 0:
        await ctx.send("❗ Loan amount must be positive.")
        return

    data = get_bank_user(ctx.author.id)
    if not data:
        await ctx.send("❗ You need to `!register` first.")
        return

    balance, loan, last_updated = data
    loan, new_time = apply_interest(loan, last_updated)

    reserve = get_main_bank_reserve()
    if reserve < amount:
        await ctx.send("❌ Bank doesn't have enough reserve to give this loan.")
        return

    balance += amount
    loan += amount
    reserve -= amount

    update_bank_user(user_id, balance=balance, loan=loan, loan_last_updated=new_time)
    update_main_bank_reserve(reserve)
    log_transaction(user_id, 'loan', amount)

    await ctx.send(f"✅ Loan of {amount:.2f} coins granted.\n💳 New Loan: {loan:.2f}\n💰 New Balance: {balance:.2f}")

@bot.command()
async def repay(ctx, amount: float):
    """
    Allows a user to repay their loan.
    """
    user_id = str(ctx.author.id)
    if amount <= 0:
        await ctx.send("❗ Repayment amount must be positive.")
        return

    data = get_bank_user(ctx.author.id)
    if not data:
        await ctx.send("❗ You need to `!register` first.")
        return

    balance, loan, last_updated = data
    loan, new_time = apply_interest(loan, last_updated)

    if balance < amount:
        await ctx.send(f"❌ You only have {balance:.2f} coins.")
        return

    if amount > loan:
        amount = loan

    balance -= amount
    loan -= amount
    reserve = get_main_bank_reserve() + amount

    update_bank_user(user_id, balance=balance, loan=loan, loan_last_updated=new_time)
    update_main_bank_reserve(reserve)
    log_transaction(user_id, 'repay', amount)

    await ctx.send(f"✅ Repaid {amount:.2f} coins.\n💳 Remaining Loan: {loan:.2f}\n💰 Balance: {balance:.2f}")

@bot.command()
async def give(ctx, member: discord.Member, amount: float):
    """
    Allows a user to give coins to another registered user.
    """
    sender_id = str(ctx.author.id)
    receiver_id = str(member.id)

    if ctx.author == member:
        await ctx.send("❗ You cannot give coins to yourself.")
        return

    if amount <= 0:
        await ctx.send("❗ Amount must be positive.")
        return

    sender_data = get_bank_user(ctx.author.id)
    receiver_data = get_bank_user(member.id)

    if not sender_data or not receiver_data:
        await ctx.send("❗ Both sender and receiver must be registered with the bank.")
        return

    sender_balance = sender_data[0]
    if sender_balance < amount:
        await ctx.send("❌ You don’t have enough coins to give.")
        return

    receiver_balance = receiver_data[0]
    sender_balance -= amount
    receiver_balance += amount

    update_bank_user_balance(int(sender_id), sender_balance)
    update_bank_user_balance(int(receiver_id), receiver_balance)
    log_transaction(sender_id, 'give', amount, receiver_id)
    log_transaction(receiver_id, 'receive', amount, sender_id)

    await ctx.send(f"✅ Gave {amount:.2f} coins to {member.mention}.")

@bot.command()
async def history(ctx):
    """
    Displays the last 10 transactions for the command invoker.
    """
    user_id = str(ctx.author.id)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        SELECT type, amount, target_user_id, timestamp FROM transactions
        WHERE user_id = ? ORDER BY timestamp DESC LIMIT 10
    ''', (user_id,))
    rows = c.fetchall()
    conn.close()

    if not rows:
        await ctx.send("📜 No transaction history found.")
        return

    message = "📜 **Last 10 Transactions:**\n"
    for t_type, amount, target, time in rows:
        target_str = f" (with <@{target}>)" if target else ""
        time_str = datetime.fromisoformat(time).strftime('%Y-%m-%d %H:%M UTC')
        message += f"- {t_type.title()}: {amount:.2f} coins{target_str} on {time_str}\n"

    await ctx.send(message)


@bot.command()
async def bankgive(ctx, member: discord.Member, amount: float):
    """
    Admin command: Gives coins to a user from the main bank's reserve.
    """
    if not is_admin(ctx):
        await ctx.send("❌ You don't have permission to use this command.")
        return

    user_id = str(member.id)
    data = get_bank_user(member.id)

    if not data:
        await ctx.send("❗ The user must be registered with the bank first.")
        return

    reserve = get_main_bank_reserve()
    if amount <= 0:
        await ctx.send("❗ Amount must be positive.")
        return
    if reserve < amount:
        await ctx.send("❌ Not enough funds in the bank reserve.")
        return

    balance = data[0] + amount
    update_bank_user_balance(int(user_id), balance)
    update_main_bank_reserve(reserve - amount)
    log_transaction(user_id, 'bankgive', amount)
    log_transaction('BANK', 'give', amount, user_id)

    await ctx.send(f"✅ {amount:.2f} coins given to {member.mention} from bank reserve.")

@bot.command()
async def take(ctx, member: discord.Member, amount: float):
    """
    Admin command: Takes coins from a user's balance.
    """
    if not is_admin(ctx):
        await ctx.send("❌ You don't have permission to use this command.")
        return

    user_id = str(member.id)
    data = get_bank_user(member.id)
    if not data:
        await ctx.send("❗ User must be registered with the bank first.")
        return

    balance = data[0]
    if amount > balance:
        amount = balance

    balance -= amount
    update_bank_user_balance(int(user_id), balance)
    log_transaction(user_id, 'take', amount)

    await ctx.send(f"✅ Took {amount:.2f} coins from {member.mention}.")

@bot.command()
async def delete(ctx, member: discord.Member):
    """
    Admin command: Deletes a user's profile from the database.
    """
    if not is_admin(ctx):
        await ctx.send("❌ You don't have permission to use this command.")
        return

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('DELETE FROM users WHERE user_id = ?', (str(member.id),))
    conn.commit()
    conn.close()
    await ctx.send(f"🗑️ {member.mention}'s bank profile deleted from the database.")

@bot.command()
async def addreserve(ctx, amount: float):
    """
    Admin command: Adds a specified amount of coins to the main bank reserve.
    Requires administrator permissions.
    """
    if not is_admin(ctx):
        await ctx.send("❌ You don't have permission to use this command.")
        return

    if amount <= 0:
        await ctx.send("❗ Amount to add must be positive.")
        return

    current_reserve = get_main_bank_reserve()
    new_reserve = current_reserve + amount
    update_main_bank_reserve(new_reserve)

    log_transaction('ADMIN', 'add_reserve', amount, 'BANK')

    await ctx.send(f"✅ {amount:.2f} coins added to the main bank reserve.\n🏦 New Reserve: {new_reserve:.2f} coins")


@bot.command(name='registershowdown')
async def register_showdown(ctx, username: str):
    """
    Registers a user's Showdown username with their Discord account for tracking.
    """
    if get_showdown_player(ctx.author.id):
        await ctx.send('❗ You are already registered for Showdown tracking.')
        return
    
    if not get_bank_user(ctx.author.id):
        await ctx.send('❗ You must register with the bank first using `!register`.')
        return
    
    if register_showdown_player(ctx.author.id, username):
        await ctx.send(f'✅ Registered Showdown username **{username}** for {ctx.author.mention}.')
    else:
        await ctx.send('❌ That Showdown username or your Discord account is already linked to someone else.')

@bot.command(name='matches')
async def matches_cmd(ctx):
    """
    Displays the Showdown match statistics for the command invoker.
    """
    p = get_showdown_player(ctx.author.id)
    if not p:
        await ctx.send('❗ You are not registered for Showdown tracking. Use `!registershowdown <username>`.')
        return
    
    _, username, _, played, wins, losses = p
    await ctx.send(f'📊 **Showdown Stats for {ctx.author.mention}**\nUsername: {username}\nMatches Played: {played}\nWins: {wins}\nLosses: {losses}')

@bot.command(name='showdownfund')
async def showdownfund_cmd(ctx):
    """
    Admin command: Displays the current balance of the Showdown fund.
    """
    if not has_admin_role(ctx.author):
        await ctx.send('❌ You need the role `' + ADMIN_ROLE_NAME + '` to view the fund.')
        return
    
    fund = get_showdown_fund()
    await ctx.send(f'🏦 Showdown Fund Balance: {int(fund)} coins')

@bot.command(name='submitreplay')
async def submit_replay(ctx, replay_url: str):
    """
    Processes a Showdown replay URL, determines winner and loser,
    updates their stats, and transfers coins based on the economy rules.
    Prevents duplicate replay submissions.
    """
    normalized_replay_url = replay_url.strip()
    if normalized_replay_url.endswith('/'):
        normalized_replay_url = normalized_replay_url[:-1]
    if not normalized_replay_url.startswith('http'):
        normalized_replay_url = 'https://' + normalized_replay_url

    if is_replay_already_submitted(normalized_replay_url):
        await ctx.send('❗ This replay has already been submitted and processed.')
        return

    async with aiohttp.ClientSession() as session:
        try:
            definitive_winner_name, clean_player_names_from_log = await parse_replay(session, normalized_replay_url)
        except ValueError as e:
            await ctx.send(f'❌ Replay error: {e}')
            return

    winner_row = get_showdown_player_by_username(definitive_winner_name)
    if not winner_row:
        await ctx.send(f'❌ Winner **{definitive_winner_name}** is not registered for Showdown tracking. They must run `!registershowdown <username>`.')
        return

    winner_id, winner_username, winner_matches_prev, winner_wins_prev, winner_losses_prev = winner_row

    loser_row = None
    for candidate_username in clean_player_names_from_log:
        if candidate_username.lower() == definitive_winner_name.lower():
            continue
        r = get_showdown_player_by_username(candidate_username)
        if r:
            loser_row = r
            break

    if not loser_row:
        await ctx.send('❌ Could not identify a registered loser in that replay (is the opponent registered with `!registershowdown`?).')
        return

    loser_id, loser_username, loser_matches_prev, loser_wins_prev, loser_losses_prev = loser_row

    winner_bank = get_bank_user(int(winner_id))
    loser_bank = get_bank_user(int(loser_id))
    if not winner_bank or not loser_bank:
        await ctx.send('❌ Both winner and loser must be registered in the main bank economy using `!register`.')
        return

    win_amount, lose_amount_requested, W_after, L_after = compute_rewards(winner_matches_prev, loser_matches_prev)

    fund = get_showdown_fund()
    if fund < win_amount:
        await ctx.send(f'🚫 Insufficient showdown fund to pay winner. Needed {win_amount}, available {int(fund)}. No changes applied.')
        return

    loser_balance, _, _ = loser_bank
    winner_balance, _, _ = winner_bank
    actual_loser_deduction = min(lose_amount_requested, loser_balance)

    new_loser_balance = loser_balance - actual_loser_deduction
    new_winner_balance = winner_balance + win_amount

    update_bank_user_balance(int(loser_id), new_loser_balance)
    update_bank_user_balance(int(winner_id), new_winner_balance)

    new_fund = fund - win_amount + actual_loser_deduction
    update_showdown_fund(new_fund)

    update_showdown_stats(int(winner_id), int(loser_id), win_amount, lose_amount_requested, actual_loser_deduction, W_after, L_after, normalized_replay_url)

    log_transaction(str(winner_id), 'showdown_win', win_amount, str(loser_id))
    if actual_loser_deduction > 0:
        log_transaction(str(loser_id), 'showdown_loss', actual_loser_deduction, str(winner_id))
    log_transaction('SHOWDOWN_FUND', 'fund_out', win_amount, str(winner_id))
    if actual_loser_deduction > 0:
        log_transaction('SHOWDOWN_FUND', 'fund_in', actual_loser_deduction, str(loser_id))

    partial_note = ''
    if actual_loser_deduction < lose_amount_requested:
        partial_note = f"\n⚠️ Loser only had {int(loser_balance)} coins. Deducted {actual_loser_deduction} instead of requested {lose_amount_requested}."

    await ctx.send(
        f'🎮 **Replay Processed**\nWinner: <@{winner_id}> ({winner_username})\nLoser: <@{loser_id}> ({loser_username})\n🏅 Winner Reward: {win_amount} coins\n💢 Loser Penalty Requested: {lose_amount_requested} coins\n💢 Loser Penalty Taken: {actual_loser_deduction} coins{partial_note}\n📊 Winner Matches (after): {W_after}\n📊 Loser Matches (after): {L_after}\n🏦 Fund Remaining: {int(new_fund)} coins')

@bot.command()
async def updateshowdownusername(ctx, member: discord.Member, new_username: str):
    """
    Admin command: Updates the registered Showdown username for a specific Discord member.
    Requires administrator permissions.
    """
    if not has_admin_role(ctx.author):
        await ctx.send("❌ You don't have permission to use this command.")
        return

    discord_id = str(member.id)
    new_username_lower = new_username.lower()

    player_data = get_showdown_player(member.id)
    if not player_data:
        await ctx.send(f"❗ {member.mention} is not registered for Showdown tracking.")
        return

    existing_player_with_new_username = get_showdown_player_by_username(new_username)
    if existing_player_with_new_username and existing_player_with_new_username[0] != discord_id:
        await ctx.send(f"❌ The Showdown username **{new_username}** is already linked to another Discord user.")
        return

    conn = db_connect()
    c = conn.cursor()
    try:
        c.execute('''
            UPDATE showdown_players
            SET showdown_username = ?, showdown_username_lower = ?
            WHERE discord_id = ?
        ''', (new_username, new_username_lower, discord_id))
        conn.commit()
        await ctx.send(f"✅ Showdown username for {member.mention} updated to **{new_username}**.")
    except Exception as e:
        await ctx.send(f"❌ An error occurred while updating the username: {e}")
        conn.rollback()
    finally:
        conn.close()

@bot.command(name='resetmatches')
async def reset_matches(ctx, member: discord.Member):
    """
    Admin command: Resets a member's Showdown matches, wins, and losses to 0.
    """
    if not has_admin_role(ctx.author):
        await ctx.send("❌ You don't have permission to use this command.")
        return

    player_data = get_showdown_player(member.id)
    if not player_data:
        await ctx.send(f"❗ {member.mention} is not registered for Showdown tracking.")
        return

    reset_showdown_stats(str(member.id))

    await ctx.send(f"✅ Showdown match history for {member.mention} has been reset to 0.")


init_db()
check_db_integrity()

bot.run("token")
