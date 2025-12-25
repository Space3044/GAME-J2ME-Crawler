import json
import os
import time
from datetime import datetime

TASKS_DIR = 'tasks'


class TaskManager:
    def __init__(self, tasks_dir=TASKS_DIR):
        self.tasks_dir = tasks_dir
        if not os.path.exists(self.tasks_dir):
            os.makedirs(self.tasks_dir)

    def _get_file_path(self, filename):
        return os.path.join(self.tasks_dir, filename)

    def list_tasks(self):
        files = [f for f in os.listdir(self.tasks_dir) if f.endswith('.json')]
        tasks = []
        for f in files:
            try:
                with open(self._get_file_path(f), 'r', encoding='utf-8') as file:
                    data = json.load(file)
                    tasks.append({
                        'filename': f,
                        'name': data.get('name', 'Unknown'),
                        'start_id': data.get('start_id'),
                        'end_id': data.get('end_id'),
                        'current_id': data.get('current_id'),
                        'status': data.get('status', 'unknown'),
                        'created_at': data.get('created_at'),
                        'count': len(data.get('data', [])),
                        'failed_count': len(data.get('failed_ids', []))
                    })
            except Exception as e:
                print(f"Error reading task {f}: {e}")

        # Sort by creation time desc
        tasks.sort(key=lambda x: x.get('created_at', ''), reverse=True)
        return tasks

    def check_task_exists(self, start_id, end_id):
        tasks = self.list_tasks()
        for task in tasks:
            if task['start_id'] == start_id and task['end_id'] == end_id:
                return task
        return None

    def create_task(self, start_id, end_id, name=None):
        # Check for duplicates
        existing = self.check_task_exists(start_id, end_id)
        if existing:
            return None, {'error': 'exists', 'task': existing}

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        if not name:
            name = "Task"  # Default name without IDs

        # New naming convention: Name_Start_End.json
        # Sanitize name to be safe for filename
        safe_name = "".join([c for c in name if c.isalnum() or c in (
            ' ', '-', '_')]).strip().replace(' ', '_')

        # Check if name already contains the ID range to avoid duplication
        suffix = f"_{start_id}_{end_id}"
        if safe_name.endswith(suffix):
            filename = f"{safe_name}.json"
        else:
            filename = f"{safe_name}_{start_id}_{end_id}.json"

        task_data = {
            'name': name,
            'filename': filename,
            'start_id': int(start_id),
            'end_id': int(end_id),
            'current_id': int(start_id),
            'status': 'ready',
            'created_at': timestamp,
            'data': [],
            'failed_ids': [],
            'custom_queue': [],
            'delay': 1.0
        }

        self.save_task(filename, task_data)
        return filename, task_data

    def rename_task(self, old_filename, new_name):
        task_data = self.load_task(old_filename)
        if not task_data:
            return None, "Task not found"

        start_id = task_data['start_id']
        end_id = task_data['end_id']

        # Generate new filename
        safe_name = "".join([c for c in new_name if c.isalnum() or c in (
            ' ', '-', '_')]).strip().replace(' ', '_')

        # Check if name already contains the ID range to avoid duplication
        suffix = f"_{start_id}_{end_id}"
        if safe_name.endswith(suffix):
            new_filename = f"{safe_name}.json"
        else:
            new_filename = f"{safe_name}_{start_id}_{end_id}.json"

        if new_filename == old_filename:
            # Just update the display name inside
            task_data['name'] = new_name
            self.save_task(old_filename, task_data)
            return old_filename, None

        # Check if new filename already exists (unlikely unless name collision)
        if os.path.exists(self._get_file_path(new_filename)):
            return None, "A task with this name and range already exists"

        # Update data
        task_data['name'] = new_name
        task_data['filename'] = new_filename

        # Save new file
        self.save_task(new_filename, task_data)

        # Delete old file
        self.delete_task(old_filename)

        return new_filename, None

    def load_task(self, filename):
        path = self._get_file_path(filename)
        if os.path.exists(path):
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        return None

    def save_task(self, filename, data):
        path = self._get_file_path(filename)
        # Ensure data is sorted by ID before saving
        if 'data' in data and isinstance(data['data'], list):
            data['data'].sort(key=lambda x: x.get('ID', 0))

        # Atomic write: write to temp file then rename
        temp_path = path + '.tmp'
        try:
            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            # Rename is atomic on POSIX, and atomic replace on Windows (Python 3.3+)
            os.replace(temp_path, path)
        except Exception as e:
            print(f"Error saving task {filename}: {e}")
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except:
                    pass

    def delete_task(self, filename):
        path = self._get_file_path(filename)
        if os.path.exists(path):
            os.remove(path)
            return True
        return False

    def update_task_metadata(self, old_filename, new_name, new_start_id, new_end_id):
        task_data = self.load_task(old_filename)
        if not task_data:
            return None, "Task not found"

        # Sanitize name
        safe_name = "".join([c for c in new_name if c.isalnum() or c in (
            ' ', '-', '_')]).strip().replace(' ', '_')

        # Check if name already contains the ID range to avoid duplication
        suffix = f"_{new_start_id}_{new_end_id}"
        if safe_name.endswith(suffix):
            new_filename = f"{safe_name}.json"
        else:
            new_filename = f"{safe_name}_{new_start_id}_{new_end_id}.json"

        # Check if filename changes and if target exists
        if new_filename != old_filename and os.path.exists(self._get_file_path(new_filename)):
            return None, "A task with this name and range already exists"

        # Update data
        task_data['name'] = new_name
        task_data['start_id'] = int(new_start_id)
        task_data['end_id'] = int(new_end_id)
        task_data['filename'] = new_filename

        # If start/end changed, we might want to reset current_id if it's out of bounds?
        # But user said "edit task", implying just metadata update.
        # We'll leave current_id alone unless it's completely invalid, but let's trust the user.

        self.save_task(new_filename, task_data)

        if new_filename != old_filename:
            self.delete_task(old_filename)

        return new_filename, None
