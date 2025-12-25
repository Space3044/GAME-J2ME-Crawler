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
                        'task_type': data.get('task_type', 'unknown'),
                        'target_name': data.get('target_name', ''),
                        'start_page': data.get('start_page'),
                        'end_page': data.get('end_page'),
                        'current_page': data.get('current_page'),
                        'status': data.get('status', 'unknown'),
                        'created_at': data.get('created_at'),
                        'count': len(data.get('data', [])),
                        'failed_count': len(data.get('failed_ids', []))
                    })
            except Exception as e:
                print(f"Error reading task {f}: {e}")

        # 按创建时间降序排序
        tasks.sort(key=lambda x: x.get('created_at', ''), reverse=True)
        return tasks

    def check_task_exists(self, start_id, end_id):
        tasks = self.list_tasks()
        for task in tasks:
            if task['start_id'] == start_id and task['end_id'] == end_id:
                return task
        return None

    def create_task(self, task_type, target_name, start_page, end_page, name=None):
        # 任务类型: 'series' 或 'console'
        # 目标名称: 'mario', 'nes' 等

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        if not name:
            name = f"{task_type}_{target_name}"

        # 净化名称
        safe_name = "".join([c for c in name if c.isalnum() or c in (
            ' ', '-', '_')]).strip().replace(' ', '_')

        # 文件名: Name_pStart_pEnd.json
        filename = f"{safe_name}_p{start_page}_p{end_page}.json"

        task_data = {
            'name': name,
            'filename': filename,
            'task_type': task_type,
            'target_name': target_name,
            'start_page': int(start_page),
            'end_page': int(end_page),
            'current_page': int(start_page),
            'status': 'ready',
            'created_at': timestamp,
            'data': [],
            'discovered_ids': [],  # 所有已发现的ID列表
            'failed_ids': [],     # 失败的游戏ID
            'failed_pages': [],   # 失败的列表页
            'custom_queue': [],
            'delay': 1.0
        }

        self.save_task(filename, task_data)
        return filename, task_data

    def update_task_metadata(self, old_filename, new_name, new_start_page, new_end_page, new_task_type=None, new_target_name=None):
        task_data = self.load_task(old_filename)
        if not task_data:
            return None, "Task not found"

        # 更新字段
        task_data['start_page'] = int(new_start_page)
        task_data['end_page'] = int(new_end_page)

        if new_task_type:
            task_data['task_type'] = new_task_type
        if new_target_name:
            task_data['target_name'] = new_target_name

        # 如果未提供名称或为空，则根据类型/目标重新生成
        if not new_name:
            new_name = f"{task_data['task_type']}_{task_data['target_name']}"

        task_data['name'] = new_name

        # 生成新文件名
        safe_name = "".join([c for c in new_name if c.isalnum() or c in (
            ' ', '-', '_')]).strip().replace(' ', '_')

        new_filename = f"{safe_name}_p{new_start_page}_p{new_end_page}.json"

        if new_filename == old_filename:
            # 仅更新内容
            self.save_task(old_filename, task_data)
            return old_filename, None

        # 检查新文件名是否已存在
        if os.path.exists(self._get_file_path(new_filename)):
            return None, "A task with this configuration already exists"

        # 更新数据中的文件名
        task_data['filename'] = new_filename

        # 保存新文件
        self.save_task(new_filename, task_data)

        # 删除旧文件
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
        # 保存前确保数据按ID排序
        if 'data' in data and isinstance(data['data'], list):
            data['data'].sort(key=lambda x: x.get('ID', 0))

        # 原子写入：写入临时文件然后重命名
        temp_path = path + '.tmp'
        try:
            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            # 重命名在POSIX上是原子的，在Windows上也是原子替换 (Python 3.3+)
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
