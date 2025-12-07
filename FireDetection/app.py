from flask import Flask, Response, render_template, jsonify, request
import cv2
from ultralytics import YOLO
import threading
import queue
import time
import json
import db  # Import database module

app = Flask(__name__)
RTSP_URL = "rtsp://admin:admin123456@192.168.1.22:8554/profile0"
MODEL_PATH = "firedetectionYolo12.pt"

# Initialize database on startup
db.init_db()

model = YOLO(MODEL_PATH)
frame_queue = queue.Queue(maxsize=2)
detection_queue = queue.Queue(maxsize=10)
stop_event = threading.Event()
capturing = False

classes = ["fire", "light", "no-fire", "smoke"]
colors = [(0, 0, 255), (0, 255, 255), (0, 255, 0), (128, 128, 128)]

# Global detection counters and session tracking
current_detections = {
    "fire": 0,
    "smoke": 0,
    "timestamp": time.time()
}
current_session_id = None

def capture_frames():
    """Capture frames from RTSP stream with error handling"""
    while not stop_event.is_set():
        if not capturing:
            time.sleep(0.1)
            continue
            
        try:
            # Reinitialize capture with optimized settings
            cap = cv2.VideoCapture(RTSP_URL, cv2.CAP_FFMPEG)
            
            # Optimize buffer and settings for RTSP
            cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
            cap.set(cv2.CAP_PROP_FPS, 15)  # Limit FPS to reduce load
            
            # Add timeout for read operations
            cap.set(cv2.CAP_PROP_OPEN_TIMEOUT_MSEC, 5000)
            cap.set(cv2.CAP_PROP_READ_TIMEOUT_MSEC, 5000)
            
            if not cap.isOpened():
                print("Failed to open RTSP stream. Retrying...")
                time.sleep(2)
                continue
            
            print("RTSP stream connected successfully!")
            consecutive_failures = 0
            
            while not stop_event.is_set() and capturing:
                ret, frame = cap.read()
                
                if not ret:
                    consecutive_failures += 1
                    print(f"Failed to read frame. Attempt {consecutive_failures}")
                    
                    # If too many failures, reconnect
                    if consecutive_failures > 10:
                        print("Too many failures. Reconnecting...")
                        break
                    
                    time.sleep(0.1)
                    continue
                
                # Reset failure counter on success
                consecutive_failures = 0
                
                # Clear old frame if queue is full
                if frame_queue.full():
                    try:
                        frame_queue.get_nowait()
                    except queue.Empty:
                        pass
                
                frame_queue.put(frame)
                time.sleep(0.03)  # ~30 FPS
            
            cap.release()
            
        except Exception as e:
            print(f"Error in capture_frames: {e}")
            time.sleep(2)  # Wait before retry

def generate():
    global current_detections, current_session_id
    
    while True:
        if not capturing:
            time.sleep(0.1)
            continue
        try:
            frame = frame_queue.get(timeout=1)
            
            # Skip frame if it's corrupted (too small or invalid)
            if frame is None or frame.size == 0:
                continue
            
            # Run YOLO detection
            results = model(frame, conf=0.4, iou=0.45, verbose=False)[0]  # verbose=False to reduce console spam
            
            fire_count = 0
            smoke_count = 0
            
            for box in results.boxes:
                cls = int(box.cls[0])
                
                # Skip "no-fire" and "light" classes
                if cls == 2 or cls == 1:  # "no-fire" or "light"
                    continue
                
                # Count detections
                if cls == 0:  # fire
                    fire_count += 1
                elif cls == 3:  # smoke
                    smoke_count += 1
                    
                x1, y1, x2, y2 = map(int, box.xyxy[0])
                conf = box.conf[0]
                label = f"{classes[cls]} {conf:.2f}"
                color = colors[cls]
                
                # Draw bounding box
                cv2.rectangle(frame, (x1, y1), (x2, y2), color, 3)
                
                # Draw label with background
                label_size = cv2.getTextSize(label, cv2.FONT_HERSHEY_DUPLEX, 0.8, 2)[0]
                cv2.rectangle(frame, (x1, y1 - label_size[1] - 10), 
                            (x1 + label_size[0], y1), color, -1)
                cv2.putText(frame, label, (x1, y1 - 10), 
                           cv2.FONT_HERSHEY_DUPLEX, 0.8, (255, 255, 255), 2)
            
            # Update detection counts if changed
            if fire_count != current_detections["fire"] or smoke_count != current_detections["smoke"]:
                current_detections = {
                    "fire": fire_count,
                    "smoke": smoke_count,
                    "timestamp": time.time()
                }
                
                # Log to database if session is active
                if current_session_id:
                    alert_triggered = fire_count > 0 or smoke_count > 0
                    db.add_detection_log(current_session_id, fire_count, smoke_count, alert_triggered)
                
                # Put detection event in queue
                if not detection_queue.full():
                    detection_queue.put(current_detections.copy())

            # Encode frame as JPEG
            _, buf = cv2.imencode('.jpg', frame, [int(cv2.IMWRITE_JPEG_QUALITY), 85])
            yield (b'--frame\r\nContent-Type: image/jpeg\r\n\r\n' + buf.tobytes() + b'\r\n')
            
        except queue.Empty:
            # No frame available, skip
            continue
        except Exception as e:
            print(f"Error in generate: {e}")
            continue

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/reports')
def reports():
    return render_template('reports.html')

@app.route('/video')
def video():
    return Response(generate(), mimetype='multipart/x-mixed-replace; boundary=frame')

@app.route('/detections')
def get_detections():
    """Server-Sent Events endpoint for real-time detection updates"""
    def event_stream():
        while True:
            if not capturing:
                time.sleep(0.5)
                continue
            try:
                # Wait for new detection with timeout
                detection = detection_queue.get(timeout=1)
                data = json.dumps(detection)
                yield f"data: {data}\n\n"
            except queue.Empty:
                # Send heartbeat to keep connection alive
                yield f": heartbeat\n\n"
            except:
                break
    
    return Response(event_stream(), mimetype='text/event-stream')

@app.route('/detection_status')
def detection_status():
    """REST endpoint to get current detection status"""
    return jsonify(current_detections)

@app.route('/start')
def start():
    global capturing, current_detections, current_session_id
    capturing = True
    
    # Start a new session in database
    current_session_id = db.start_session()
    
    # Reset counters on start
    current_detections = {
        "fire": 0,
        "smoke": 0,
        "timestamp": time.time()
    }
    # Clear detection queue
    while not detection_queue.empty():
        try:
            detection_queue.get_nowait()
        except queue.Empty:
            break
    return jsonify({"status": "ok", "session_id": current_session_id})

@app.route('/stop')
def stop():
    global capturing, current_detections, current_session_id
    capturing = False
    
    # End the current session in database
    if current_session_id:
        db.end_session(current_session_id)
        current_session_id = None
    
    # Reset counters on stop
    current_detections = {
        "fire": 0,
        "smoke": 0,
        "timestamp": time.time()
    }
    return "OK"

# Database API Routes
@app.route('/api/statistics')
def get_statistics():
    """Get overall statistics"""
    stats = db.get_statistics()
    return jsonify(stats)

@app.route('/api/sessions')
def get_sessions():
    """Get all detection sessions"""
    limit = request.args.get('limit', 50, type=int)
    sessions = db.get_sessions(limit)
    return jsonify(sessions)

@app.route('/api/session/<int:session_id>')
def get_session_details(session_id):
    """Get details of a specific session"""
    logs = db.get_session_logs(session_id)
    return jsonify(logs)

@app.route('/api/reports/date-range')
def get_reports_by_date():
    """Get reports within a date range"""
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    detections = db.get_detections_by_date(start_date, end_date)
    return jsonify(detections)

if __name__ == '__main__':
    threading.Thread(target=capture_frames, daemon=True).start()
    app.run(host='0.0.0.0', port=5000, threaded=True)