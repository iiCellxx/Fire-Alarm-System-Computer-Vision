import sqlite3
from datetime import datetime
import os

DATABASE_NAME = 'fire_detection.db'

def init_db():
    """Initialize the database and create tables if they don't exist"""
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    
    # Create detections table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS detections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            detection_type TEXT NOT NULL,
            count INTEGER NOT NULL,
            confidence REAL,
            image_path TEXT
        )
    ''')
    
    # Create detection_sessions table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS detection_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            start_time DATETIME DEFAULT CURRENT_TIMESTAMP,
            end_time DATETIME,
            total_fire_detections INTEGER DEFAULT 0,
            total_smoke_detections INTEGER DEFAULT 0,
            status TEXT DEFAULT 'active'
        )
    ''')
    
    # Create detection_logs table for detailed logs
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS detection_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            fire_count INTEGER DEFAULT 0,
            smoke_count INTEGER DEFAULT 0,
            alert_triggered BOOLEAN DEFAULT 0,
            FOREIGN KEY (session_id) REFERENCES detection_sessions(id)
        )
    ''')
    
    conn.commit()
    conn.close()
    print("Database initialized successfully!")

def add_detection(detection_type, count, confidence=None, image_path=None):
    """Add a new detection record"""
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    
    cursor.execute('''
        INSERT INTO detections (detection_type, count, confidence, image_path)
        VALUES (?, ?, ?, ?)
    ''', (detection_type, count, confidence, image_path))
    
    conn.commit()
    detection_id = cursor.lastrowid
    conn.close()
    return detection_id

def start_session():
    """Start a new detection session"""
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    
    cursor.execute('''
        INSERT INTO detection_sessions (start_time, status)
        VALUES (?, 'active')
    ''', (datetime.now(),))
    
    conn.commit()
    session_id = cursor.lastrowid
    conn.close()
    return session_id

def end_session(session_id):
    """End a detection session"""
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    
    # Calculate total detections for this session
    cursor.execute('''
        SELECT 
            SUM(fire_count) as total_fire,
            SUM(smoke_count) as total_smoke
        FROM detection_logs
        WHERE session_id = ?
    ''', (session_id,))
    
    result = cursor.fetchone()
    total_fire = result[0] or 0
    total_smoke = result[1] or 0
    
    # Update session
    cursor.execute('''
        UPDATE detection_sessions
        SET end_time = ?,
            total_fire_detections = ?,
            total_smoke_detections = ?,
            status = 'completed'
        WHERE id = ?
    ''', (datetime.now(), total_fire, total_smoke, session_id))
    
    conn.commit()
    conn.close()

def add_detection_log(session_id, fire_count, smoke_count, alert_triggered=False):
    """Add a detection log entry"""
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    
    cursor.execute('''
        INSERT INTO detection_logs (session_id, fire_count, smoke_count, alert_triggered)
        VALUES (?, ?, ?, ?)
    ''', (session_id, fire_count, smoke_count, alert_triggered))
    
    conn.commit()
    conn.close()

def get_all_detections(limit=100):
    """Get all detection records"""
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT id, timestamp, detection_type, count, confidence
        FROM detections
        ORDER BY timestamp DESC
        LIMIT ?
    ''', (limit,))
    
    results = cursor.fetchall()
    conn.close()
    
    return [
        {
            'id': row[0],
            'timestamp': row[1],
            'type': row[2],
            'count': row[3],
            'confidence': row[4]
        }
        for row in results
    ]

def get_sessions(limit=50):
    """Get all detection sessions"""
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT id, start_time, end_time, total_fire_detections, 
               total_smoke_detections, status
        FROM detection_sessions
        ORDER BY start_time DESC
        LIMIT ?
    ''', (limit,))
    
    results = cursor.fetchall()
    conn.close()
    
    return [
        {
            'id': row[0],
            'start_time': row[1],
            'end_time': row[2],
            'total_fire': row[3],
            'total_smoke': row[4],
            'status': row[5]
        }
        for row in results
    ]

def get_session_logs(session_id):
    """Get all logs for a specific session"""
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT id, timestamp, fire_count, smoke_count, alert_triggered
        FROM detection_logs
        WHERE session_id = ?
        ORDER BY timestamp ASC
    ''', (session_id,))
    
    results = cursor.fetchall()
    conn.close()
    
    return [
        {
            'id': row[0],
            'timestamp': row[1],
            'fire_count': row[2],
            'smoke_count': row[3],
            'alert_triggered': row[4]
        }
        for row in results
    ]

def get_statistics():
    """Get overall statistics"""
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    
    # Total sessions
    cursor.execute('SELECT COUNT(*) FROM detection_sessions')
    total_sessions = cursor.fetchone()[0]
    
    # Total detections
    cursor.execute('''
        SELECT 
            SUM(total_fire_detections) as total_fire,
            SUM(total_smoke_detections) as total_smoke
        FROM detection_sessions
    ''')
    result = cursor.fetchone()
    total_fire = result[0] or 0
    total_smoke = result[1] or 0
    
    # Recent activity (last 24 hours)
    cursor.execute('''
        SELECT COUNT(*) 
        FROM detection_sessions
        WHERE start_time >= datetime('now', '-1 day')
    ''')
    recent_sessions = cursor.fetchone()[0]
    
    # Average detections per session
    cursor.execute('''
        SELECT 
            AVG(total_fire_detections) as avg_fire,
            AVG(total_smoke_detections) as avg_smoke
        FROM detection_sessions
        WHERE status = 'completed'
    ''')
    result = cursor.fetchone()
    avg_fire = round(result[0] or 0, 2)
    avg_smoke = round(result[1] or 0, 2)
    
    conn.close()
    
    return {
        'total_sessions': total_sessions,
        'total_fire_detections': total_fire,
        'total_smoke_detections': total_smoke,
        'recent_sessions': recent_sessions,
        'avg_fire_per_session': avg_fire,
        'avg_smoke_per_session': avg_smoke
    }

def get_detections_by_date(start_date=None, end_date=None):
    """Get detections within a date range"""
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    
    if start_date and end_date:
        cursor.execute('''
            SELECT id, start_time, end_time, total_fire_detections, 
                   total_smoke_detections, status
            FROM detection_sessions
            WHERE start_time BETWEEN ? AND ?
            ORDER BY start_time DESC
        ''', (start_date, end_date))
    else:
        cursor.execute('''
            SELECT id, start_time, end_time, total_fire_detections, 
                   total_smoke_detections, status
            FROM detection_sessions
            ORDER BY start_time DESC
            LIMIT 100
        ''')
    
    results = cursor.fetchall()
    conn.close()
    
    return [
        {
            'id': row[0],
            'start_time': row[1],
            'end_time': row[2],
            'total_fire': row[3],
            'total_smoke': row[4],
            'status': row[5]
        }
        for row in results
    ]

def delete_old_records(days=30):
    """Delete records older than specified days"""
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    
    cursor.execute('''
        DELETE FROM detection_logs
        WHERE session_id IN (
            SELECT id FROM detection_sessions
            WHERE start_time < datetime('now', '-' || ? || ' days')
        )
    ''', (days,))
    
    cursor.execute('''
        DELETE FROM detection_sessions
        WHERE start_time < datetime('now', '-' || ? || ' days')
    ''', (days,))
    
    cursor.execute('''
        DELETE FROM detections
        WHERE timestamp < datetime('now', '-' || ? || ' days')
    ''', (days,))
    
    conn.commit()
    deleted_count = cursor.rowcount
    conn.close()
    
    return deleted_count

if __name__ == '__main__':
    # Initialize database when running this file directly
    init_db()
    print("Database tables created successfully!")