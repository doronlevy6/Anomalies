from flask import Flask, render_template, jsonify
# from workflow_logic import run_workflow 
# We import inside the route to ensuring fresh reloading if needed, 
# although not strictly necessary for simple app
# import anomalies_logic # Imported below

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

import threading
import json
import time
from flask import Flask, render_template, jsonify, Response, stream_with_context

# from workflow_logic import run_workflow 
import anomalies_logic
import alerts_logic

app = Flask(__name__)

# --- Global Job State ---
class JobManager:
    def __init__(self):
        self.active_context = None
        self.thread = None
        self.lock = threading.Lock()

    def start_job(self, mode='anomalies', limit=15):
        with self.lock:
            if self.active_context and self.thread and self.thread.is_alive():
                return False, "Job already running"
            
            self.active_context = anomalies_logic.WorkflowContext()
            
            def wrapper(ctx):
                try:
                    if mode == 'alerts':
                        cards = alerts_logic.run_alerts_workflow(ctx)
                    else:
                        cards = anomalies_logic.run_anomalies_workflow(ctx, limit=limit)
                    
                    ctx.msg_queue.put({"type": "result", "cards": cards})
                except Exception as e:
                    ctx.msg_queue.put({"type": "error", "message": str(e)})
                finally:
                    ctx.msg_queue.put({"type": "done"})

            self.thread = threading.Thread(target=wrapper, args=(self.active_context,))
            self.thread.start()
            return True, "Job started"
            
    def stop_job(self):
        with self.lock:
            if self.active_context:
                self.active_context.request_stop()
                return True
            return False

    def get_stream(self):
        # Locate the current context (snapshot)
        ctx = self.active_context
        if not ctx:
            yield "data: {}\n\n"
            return

        while True:
            try:
                # Blok for 0.5s to get message, else checking alive
                msg = ctx.msg_queue.get(timeout=0.5)
                yield f"data: {json.dumps(msg)}\n\n"
                if msg.get("type") in ["done", "error"]:
                    break
            except:
                # Queue empty, check if thread died silently or just waiting
                if self.thread and not self.thread.is_alive() and ctx.msg_queue.empty():
                     yield f"data: {json.dumps({'type': 'done'})}\n\n"
                     break
                continue

job_manager = JobManager()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/run', methods=['POST'])
def run_analysis():
    data = request.json or {}
    limit = data.get('limit', 15)
    success, msg = job_manager.start_job(mode='anomalies', limit=limit)
    if success:
        return jsonify({"status": "started"}), 202
    else:
        return jsonify({"status": "busy", "message": msg}), 409

@app.route('/api/run-alerts', methods=['POST'])
def run_alerts():
    success, msg = job_manager.start_job(mode='alerts')
    if success:
        return jsonify({"status": "started"}), 202
    else:
        return jsonify({"status": "busy", "message": msg}), 409

@app.route('/api/stop', methods=['POST'])
def stop_analysis():
    if job_manager.stop_job():
        return jsonify({"status": "stopped"})
    return jsonify({"status": "no_active_job"}), 400

@app.route('/api/stream')
def stream():
    return Response(stream_with_context(job_manager.get_stream()), mimetype="text/event-stream")

@app.route('/api/reload-map', methods=['POST'])
def reload_map():
    try:
        count = anomalies_logic.load_account_map()
        alerts_logic.load_account_map_independent()
        
        # Auto-open the file for visibility
        excel_path = os.path.join(os.path.dirname(__file__), 'templates', 'mailsToFlow1.xlsx')
        if os.path.exists(excel_path):
            try:
                subprocess.run(['open', excel_path]) # macOS specific
            except Exception as ex:
                print(f"Failed to open Excel: {ex}")
                
        return jsonify({
            "status": "success", 
            "count": count, 
            "message": f"Successfully reloaded {count} accounts from Excel (updated in both workflows).",
            "data": anomalies_logic.get_account_map()
        })
    except Exception as e:
        return jsonify({"status": "error", "message": f"Error reloading map: {str(e)}"}), 500

from flask import request
import export_helper

@app.route('/api/export-anomaly', methods=['POST'])
def export_anomaly():
    """Export anomaly to Excel tracking files"""
    data = request.json
    force = data.get('force', False)
    
    try:
        result = export_helper.export_anomaly(data, force_master=force)
        
        if result['status'] == 'success':
            return jsonify({
                "status": "success", 
                "message": "אנומליה נוספה לקבצי המעקב (יומי וראשי)",
                "timestamp": result.get('timestamp')
            })
        elif result['status'] == 'daily_duplicate':
            return jsonify({
                "status": "daily_duplicate", 
                "message": "אנומליה זו כבר קיימת בקובץ היומי (נחסם)",
                "timestamp": result.get('timestamp')
            })
        elif result['status'] == 'master_duplicate':
            existing_date = result.get('existing_date', 'Unknown')
            return jsonify({
                "status": "master_duplicate", 
                "message": f"אנומליה זו קיימת בהיסטוריה (הוספה ב: {existing_date}). האם להוסיף בכל זאת?",
                "existing_date": existing_date
            })
            
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/delete-rows', methods=['POST'])
def delete_rows():
    """Delete selected rows from tracking files"""
    data = request.json
    file_type = data.get('type') # 'daily' or 'master'
    timestamps = data.get('timestamps', [])
    
    if not file_type or not timestamps:
         return jsonify({"status": "error", "message": "Missing type or timestamps"}), 400
         
    try:
        success = export_helper.delete_rows(file_type, timestamps)
        if success:
             return jsonify({"status": "success", "message": f"נמחקו {len(timestamps)} שורות בהצלחה"})
        else:
             return jsonify({"status": "error", "message": "לא נמצאו שורות למחיקה"}), 404
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/clear-export', methods=['POST'])
def clear_export():
    """Clear Daily Excel file"""
    try:
        export_helper.clear_daily_file()
        return jsonify({"status": "success", "message": "קובץ המעקב היומי אופס בהצלחה"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/reset-export', methods=['POST'])
def reset_export():
    """Reset (delete) the anomaly export CSV file"""
    try:
        if os.path.exists(export_helper.EXPORT_FILE):
            os.remove(export_helper.EXPORT_FILE)
            return jsonify({"status": "success", "message": "קובץ המעקב אופס בהצלחה"})
        else:
            return jsonify({"status": "success", "message": "קובץ המעקב לא קיים"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/get-tracking-data')
def get_tracking_data():
    """Returns the daily and master tracking data"""
    try:
        data = export_helper.get_tracking_data()
        return jsonify({"status": "success", "data": data})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/update-status', methods=['POST'])
def update_status():
    """Updates the Status field for a specific row in Daily or Master table"""
    data = request.json
    timestamp = data.get('timestamp')
    new_status = data.get('status')
    file_type = data.get('file_type', 'master') # Default to master for backward compatibility
    
    if not timestamp or not new_status:
        return jsonify({"status": "error", "message": "Missing timestamp or status"}), 400
    
    try:
        success = export_helper.update_status(timestamp, new_status, file_type)
        return jsonify({"status": "success", "message": f"Status updated to: {new_status}"})
    except ValueError as ve:
        return jsonify({"status": "error", "message": str(ve)}), 404
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

import os
import subprocess

@app.route('/api/email/<message_id>')
def view_email(message_id):
    html_content = anomalies_logic.fetch_email_html(message_id)
    return html_content

@app.route('/api/open-excel', methods=['POST'])
def open_excel():
    """Open the customer data Excel file"""
    try:
        excel_path = os.path.join(os.path.dirname(__file__), 'templates', 'mailsToFlow1.xlsx')
        if not os.path.exists(excel_path):
            return jsonify({"status": "error", "message": "Excel file not found"}), 404
        
        # Open the file with the default application (macOS)
        subprocess.run(['open', excel_path], check=True)
        return jsonify({"status": "success", "message": "Excel file opened successfully"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/open-excel-file', methods=['POST'])
def open_excel_file():
    """Open daily or master Excel tracking file"""
    data = request.json
    file_type = data.get('file_type')
    
    try:
        if file_type == 'daily':
            file_path = export_helper.DAILY_FILE
            file_name = 'Daily'
        elif file_type == 'master':
            file_path = export_helper.MASTER_FILE
            file_name = 'Master'
        else:
            return jsonify({"status": "error", "message": "Invalid file type"}), 400
        
        if not os.path.exists(file_path):
            return jsonify({"status": "error", "message": f"{file_name} file not found"}), 404
        
        # Open the file with the default application (macOS)
        subprocess.run(['open', file_path], check=True)
        return jsonify({"status": "success", "message": f"{file_name} Excel file opened successfully"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/oauth2callback')
def oauth2callback():
    return "Authentication successful! You can close this window and return to the application."

if __name__ == '__main__':
    app.run(debug=True, port=5001, threaded=True)
