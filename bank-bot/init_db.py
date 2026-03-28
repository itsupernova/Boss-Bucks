import sqlite3

conn = sqlite3.connect('bank.db')
c = conn.cursor()

# Create the users table
c.execute('''
CREATE TABLE IF NOT EXISTS users (
    user_id TEXT PRIMARY KEY,
    balance INTEGER DEFAULT 0,
    loan INTEGER DEFAULT 0,
    loan_last_updated TIMESTAMP
)
''')

# Create the transactions table
c.execute('''
CREATE TABLE IF NOT EXISTS transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT,
    amount INTEGER,
    type TEXT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
''')

# Create the bank table with reserve and bot_fund
c.execute('''
CREATE TABLE IF NOT EXISTS bank (
    id INTEGER PRIMARY KEY,
    reserve INTEGER DEFAULT 0,
    bot_fund INTEGER DEFAULT 40000
)
''')

# Insert default bank row if it doesn't exist
c.execute("SELECT COUNT(*) FROM bank WHERE id = 1")
if c.fetchone()[0] == 0:
    c.execute("INSERT INTO bank (id, reserve, bot_fund) VALUES (1, 0, 40000)")

conn.commit()
conn.close()

print("Database initialized successfully.")
