from flask import Flask, render_template, jsonify, request, send_file
import threading
import time
import os
from storage import TaskManager
from crawler import Crawler
from exporter import export_task_to_excel, generate_filename

app = Flask(__name__)

# Global instances
task_manager = TaskManager()
crawler = Crawler()
active_task_filename = None


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/tasks', methods=['GET'])
def list_tasks():
    tasks = task_manager.list_tasks()
    # Mark which one is active
    for t in tasks:
        t['is_active'] = (t['filename'] == active_task_filename)

    return jsonify(tasks)


@app.route('/api/tasks', methods=['POST'])
def create_task():
    data = request.json
    start_id = data.get('start_id')
    end_id = data.get('end_id')
    name = data.get('name')

    if start_id is None or end_id is None:
        return jsonify({'error': 'Missing start_id or end_id'}), 400

    try:
        start_id = int(start_id)
        end_id = int(end_id)
    except ValueError:
        return jsonify({'error': 'IDs must be integers'}), 400

    if start_id <= 0 or end_id <= 0:
        return jsonify({'error': 'IDs must be positive integers'}), 400

    if start_id > end_id:
        return jsonify({'error': 'Start ID cannot be greater than End ID'}), 400

    result = task_manager.create_task(start_id, end_id, name)
    if result[0] is None:
        # Error occurred
        return jsonify(result[1]), 400

    filename, task_data = result
    return jsonify({'status': 'created', 'filename': filename, 'task': task_data})


@app.route('/api/tasks/<filename>/rename', methods=['POST'])
def rename_task(filename):
    global active_task_filename
    data = request.json
    new_name = data.get('name')

    if not new_name:
        return jsonify({'error': 'Missing name'}), 400

    if active_task_filename == filename and crawler.running:
        return jsonify({'error': 'Cannot rename running task'}), 400

    new_filename, error = task_manager.rename_task(filename, new_name)

    if error:
        return jsonify({'error': error}), 400

    # 如果重命名了当前活动任务，更新全局引用
    if active_task_filename == filename:
        active_task_filename = new_filename

    return jsonify({'status': 'renamed', 'filename': new_filename})


@app.route('/api/tasks/<filename>', methods=['PUT'])
def update_task(filename):
    global active_task_filename

    # 如果任务正在运行，阻止更新
    if active_task_filename == filename and crawler.running:
        return jsonify({'error': 'Cannot update running task'}), 400

    data = request.json
    new_name = data.get('name')
    new_start_id = data.get('start_id')
    new_end_id = data.get('end_id')

    if not new_name or new_start_id is None or new_end_id is None:
        return jsonify({'error': 'Missing parameters'}), 400

    try:
        new_start_id = int(new_start_id)
        new_end_id = int(new_end_id)
    except ValueError:
        return jsonify({'error': 'IDs must be integers'}), 400

    if new_start_id <= 0 or new_end_id <= 0:
        return jsonify({'error': 'IDs must be positive integers'}), 400

    if new_start_id > new_end_id:
        return jsonify({'error': 'Start ID cannot be greater than End ID'}), 400

    # 加载旧任务以便在更新前进行比较
    old_task = task_manager.load_task(filename)
    if not old_task:
        return jsonify({'error': 'Task not found'}), 404

    old_start = old_task.get('start_id')
    old_end = old_task.get('end_id')
    current_id = old_task.get('current_id')
    status = old_task.get('status')

    new_filename, error = task_manager.update_task_metadata(
        filename, new_name, new_start_id, new_end_id)
    if error:
        return jsonify({'error': error}), 400

    # 加载新任务以应用逻辑
    new_task = task_manager.load_task(new_filename)
    if new_task:
        updated_logic = False

        # 1. 处理起始ID变更 (如果新起始ID小于当前进度，重置进度)
        if int(new_start_id) < current_id:
            new_task['current_id'] = int(new_start_id)
            new_task['status'] = 'stopped'  # 如果回退，重置状态
            updated_logic = True

        # 2. 处理结束ID扩展
        if int(new_end_id) > old_end and status == 'completed':
            # 我们扩展了已完成任务的范围
            # 应该从上次结束的地方继续
            # 因为卡在 old_end，所以从 old_end + 1 开始
            new_task['current_id'] = old_end + 1
            new_task['status'] = 'stopped'
            updated_logic = True

        if updated_logic:
            task_manager.save_task(new_filename, new_task)

    if active_task_filename == filename:
        active_task_filename = new_filename

    return jsonify({'status': 'updated', 'filename': new_filename})


@app.route('/api/tasks/<filename>', methods=['GET'])
def get_task(filename):
    task = task_manager.load_task(filename)
    if not task:
        return jsonify({'error': 'Task not found'}), 404
    return jsonify(task)


@app.route('/api/tasks/<filename>', methods=['DELETE'])
def delete_task(filename):
    global active_task_filename
    if active_task_filename == filename:
        if crawler.running:
            return jsonify({'error': 'Cannot delete running task'}), 400
        active_task_filename = None

    if task_manager.delete_task(filename):
        return jsonify({'status': 'deleted'})
    return jsonify({'error': 'Task not found'}), 404


@app.route('/api/tasks/<filename>/load', methods=['POST'])
def load_task(filename):
    global active_task_filename

    if crawler.running:
        return jsonify({'error': 'Crawler is running. Stop it first.'}), 400

    task = task_manager.load_task(filename)
    if not task:
        return jsonify({'error': 'Task not found'}), 404

    active_task_filename = filename
    return jsonify({'status': 'loaded', 'task': task})


@app.route('/api/tasks/<filename>/export', methods=['GET'])
def export_task(filename):
    task = task_manager.load_task(filename)
    if not task:
        return jsonify({'error': 'Task not found'}), 404

    excel_file = export_task_to_excel(task)
    if not excel_file:
        return jsonify({'error': 'No data to export'}), 400

    download_name = generate_filename(task)

    return send_file(
        excel_file,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=download_name
    )

# --- Crawler Control API ---


def save_task_callback(task_data):
    # Use the filename from the task data if available, otherwise fall back to active_task_filename
    filename = task_data.get('filename') or active_task_filename
    if filename:
        task_manager.save_task(filename, task_data)


@app.route('/api/crawler/start', methods=['POST'])
def start_crawler():
    global active_task_filename

    if not active_task_filename:
        return jsonify({'error': 'No task loaded'}), 400

    task_data = task_manager.load_task(active_task_filename)
    if not task_data:
        return jsonify({'error': 'Task file missing'}), 404

    # If crawler is already running, just return status
    if crawler.running:
        if crawler.paused:
            crawler.resume()
            return jsonify({'status': 'resumed'})
        return jsonify({'status': 'already_running'})

    # Start new crawl session
    crawler.start(task_data, save_callback=save_task_callback)
    return jsonify({'status': 'started'})


@app.route('/api/crawler/pause', methods=['POST'])
def pause_crawler():
    crawler.pause()
    return jsonify({'status': 'paused'})


@app.route('/api/crawler/stop', methods=['POST'])
def stop_crawler():
    crawler.stop()
    return jsonify({'status': 'stopped'})


@app.route('/api/crawler/status', methods=['GET'])
def crawler_status():
    if not active_task_filename:
        return jsonify({
            'active': False,
            'message': 'No task loaded'
        })

    # 从文件加载任务数据以确保持久化
    td = task_manager.load_task(active_task_filename)
    if not td:
        return jsonify({'active': False, 'message': 'Task file not found'})

    # 如果爬虫正在运行此任务，使用内存数据更新进度字段
    # 这样可以提供流畅的UI更新，而无需频繁的磁盘I/O
    if crawler.running and crawler.task_data and crawler.task_data.get('filename') == active_task_filename:
        current_id = crawler.task_data.get('current_id')
        count = len(crawler.task_data.get('data', []))
        failed_count = len(crawler.task_data.get('failed_ids', []))
        queue_size = len(crawler.task_data.get('custom_queue', []))
    else:
        current_id = td.get('current_id')
        count = len(td.get('data', []))
        failed_count = len(td.get('failed_ids', []))
        queue_size = len(td.get('custom_queue', []))

    # 确定 display_id (显示为 "当前ID" 的内容)
    display_id = current_id

    if crawler.running and crawler.task_data and crawler.task_data.get('filename') == active_task_filename:
        if hasattr(crawler, 'processing_id') and crawler.processing_id is not None:
            display_id = crawler.processing_id
        else:
            # 爬虫正在运行但尚未选取ID（启动阶段）
            # 如果有自定义队列，显示其第一项
            cq = crawler.task_data.get('custom_queue', [])
            if cq:
                display_id = cq[0]
    elif not crawler.running:
        # 如果已停止，且队列中有项目，显示第一项
        custom_queue = td.get('custom_queue', [])
        if custom_queue:
            display_id = custom_queue[0]

    status_data = {
        'active': True,
        'filename': active_task_filename,
        'running': crawler.running,
        'paused': crawler.paused,

        # 运行时信息（日志，当前活动）
        'current_url': crawler.current_url if crawler.running else td.get('current_url', ''),
        'current_title': crawler.current_title if crawler.running else td.get('current_title', ''),
        'current_desc': crawler.current_desc if crawler.running else td.get('current_desc', ''),
        'logs': crawler.logs,

        # 进度信息（内存或文件）
        'current_id': current_id,
        'display_id': display_id,
        'total': td.get('end_id') - td.get('start_id') + 1,
        'start_id': td.get('start_id'),
        'end_id': td.get('end_id'),
        'count': count,
        'failed_count': failed_count,
        'queue_size': queue_size,
        'delay': td.get('delay', 1.0),
        'status': td.get('status')
    }

    return jsonify(status_data)


@app.route('/api/crawler/set_delay', methods=['POST'])
def set_delay():
    data = request.json
    new_delay = float(data.get('delay', 1.0))
    if new_delay < 0.1:
        new_delay = 0.1

    if crawler.task_data:
        crawler.task_data['delay'] = new_delay
    elif active_task_filename:
        # Update file directly if not running
        td = task_manager.load_task(active_task_filename)
        if td:
            td['delay'] = new_delay
            task_manager.save_task(active_task_filename, td)

    return jsonify({'status': 'updated', 'delay': new_delay})


@app.route('/api/crawler/check_integrity', methods=['POST'])
def check_integrity():
    if not active_task_filename:
        return jsonify({'error': 'No task loaded'}), 400

    # 确定使用哪个数据源
    if crawler.running and crawler.task_data and crawler.task_data.get('filename') == active_task_filename:
        td = crawler.task_data
        using_memory = True
    else:
        td = task_manager.load_task(active_task_filename)
        using_memory = False

    if not td:
        return jsonify({'error': 'Task data not found'}), 404

    start_id = td.get('start_id')
    end_id = td.get('end_id')
    data_list = td.get('data', [])
    failed_ids = td.get('failed_ids', [])

    # 1. 查找缺失的ID
    crawled_ids = set(item['ID'] for item in data_list)
    all_ids = set(range(start_id, end_id + 1))
    missing_ids = all_ids - crawled_ids

    # 2. 查找无效项目（标题为空或特定错误标题）
    invalid_ids = set()
    for item in data_list:
        title = item.get('Title', '').strip()
        if not title or title == '老游戏在线玩':
            invalid_ids.add(item['ID'])

    # 合并所有问题
    new_failed = missing_ids.union(invalid_ids)

    # 添加到 failed_ids 如果尚未存在
    added_count = 0
    for fid in new_failed:
        if fid not in failed_ids:
            failed_ids.append(fid)
            added_count += 1

    # 我们不从数据列表中删除无效项目。
    # 它们将在重新抓取时被覆盖。

    # 保存更改
    if using_memory:
        # 爬虫正在运行，它最终会保存，但我们更新了引用
        # 我们可能应该强制保存或让循环处理它。
        # 但直接修改 crawler.task_data 是安全的，因为它是一个引用。
        pass
    else:
        task_manager.save_task(active_task_filename, td)

    return jsonify({
        'status': 'checked',
        'added_count': added_count,
        'total_failed': len(failed_ids),
        'invalid_removed': len(invalid_ids)
    })


@app.route('/api/crawler/retry_failed', methods=['POST'])
def retry_failed():
    if not active_task_filename:
        return jsonify({'error': 'No task loaded'}), 400

    # 如果正在运行，更新实时数据
    if crawler.running and crawler.task_data:
        failed = crawler.task_data.get('failed_ids', [])
        if not failed:
            return jsonify({'status': 'no_failed_ids'})

        # 添加到自定义队列
        for fid in failed:
            if fid not in crawler.task_data['custom_queue']:
                crawler.task_data['custom_queue'].append(fid)

        return jsonify({'status': 'added_to_queue', 'count': len(failed)})
    else:
        # 更新文件
        td = task_manager.load_task(active_task_filename)
        if td:
            failed = td.get('failed_ids', [])
            if not failed:
                return jsonify({'status': 'no_failed_ids'})

            if 'custom_queue' not in td:
                td['custom_queue'] = []

            for fid in failed:
                if fid not in td['custom_queue']:
                    td['custom_queue'].append(fid)

            task_manager.save_task(active_task_filename, td)
            return jsonify({'status': 'added_to_queue', 'count': len(failed)})

    return jsonify({'error': 'Unknown state'}), 500


if __name__ == '__main__':
    app.run(debug=True, port=5000)
