import time
import threading
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright


class Crawler:
    def __init__(self):
        self.thread = None
        self.running = False
        self.paused = False
        self.task_data = None
        self.save_callback = None
        self.log_callback = None

        # 运行时状态（不保存到JSON）
        self.current_url = ""
        self.current_title = ""
        self.current_desc = ""
        self.processing_id = None
        self.logs = []

    def start(self, task_data, save_callback=None, log_callback=None):
        if self.running:
            return False

        self.task_data = task_data
        self.save_callback = save_callback
        self.log_callback = log_callback
        self.running = True
        self.paused = False
        self.logs = []  # 启动时清除运行时日志
        self.processing_id = None

        self.thread = threading.Thread(target=self._crawl_loop)
        self.thread.daemon = True
        self.thread.start()
        return True

    def pause(self):
        self.paused = True
        self.log("Crawler paused.")

    def resume(self):
        self.paused = False
        self.log("Crawler resumed.")

    def stop(self):
        self.running = False
        self.log("Stopping crawler...")

    def log(self, message):
        timestamp = time.strftime("%H:%M:%S")
        log_msg = f"[{timestamp}] {message}"
        self.logs.append(log_msg)
        if len(self.logs) > 100:
            self.logs.pop(0)
        if self.log_callback:
            self.log_callback(log_msg)

    def _crawl_loop(self):
        count_since_save = 0

        # 从 task_data 提取配置
        start_id = self.task_data.get('start_id')
        end_id = self.task_data.get('end_id')

        # 确保数据结构存在
        if 'data' not in self.task_data:
            self.task_data['data'] = []
        if 'failed_ids' not in self.task_data:
            self.task_data['failed_ids'] = []
        if 'custom_queue' not in self.task_data:
            self.task_data['custom_queue'] = []

        # 创建映射以便快速查找，避免重复
        data_map = {item['ID']: item for item in self.task_data['data']}

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            )
            context.set_default_timeout(15000)

            self.log(f"Started crawling task: {self.task_data.get('name')}")

            while self.running:
                if self.paused:
                    time.sleep(0.5)
                    continue

                # 确定目标ID
                target_id = None
                is_custom = False

                if self.task_data['custom_queue']:
                    target_id = self.task_data['custom_queue'].pop(0)
                    is_custom = True
                elif self.task_data['current_id'] <= end_id:
                    target_id = self.task_data['current_id']
                    is_custom = False
                else:
                    # 由于循环末尾的检查，这个分支现在很少会被命中，
                    # 但如果开始时 current_id > end_id，保留它以确保安全
                    self.log("Reached end of range.")
                    self.running = False
                    self.task_data['status'] = 'completed'
                    break

                self.processing_id = target_id

                url = f"https://zaixianwan.app/games/{target_id}"
                self.current_url = url

                # 如果已抓取则跳过（除非是重试/自定义队列）
                if not is_custom and target_id in data_map:
                    self.log(f"Skipping {target_id} (already crawled)")
                    # 与循环末尾的逻辑相同
                    if self.task_data['current_id'] < end_id:
                        self.task_data['current_id'] += 1
                    else:
                        self.task_data['status'] = 'completed'
                        self.running = False
                        self.log("Task completed (skipped last).")
                        if self.save_callback:
                            self.save_callback(self.task_data)
                        break
                    continue

                try:
                    page = context.new_page()
                    try:
                        for attempt in range(3):
                            try:
                                page.goto(url, timeout=15000,
                                          wait_until='domcontentloaded')
                                break
                            except Exception as nav_err:
                                if attempt == 2:
                                    raise nav_err
                                time.sleep(2)

                        # 等待标题
                        try:
                            page.wait_for_selector('.game-title', timeout=3000)
                        except:
                            pass

                        # 等待描述
                        try:
                            page.wait_for_selector(
                                '.description-markdown-html', timeout=2000)
                        except:
                            pass

                        content = page.content()
                        soup = BeautifulSoup(content, 'html.parser')

                        # 标题提取逻辑
                        title = ""
                        title_tag = soup.find('span', class_='game-title')

                        if title_tag:
                            title = title_tag.get_text(strip=True)

                        if not title:
                            # 如果标题缺失，视为失败
                            raise Exception(
                                "Title not found (.game-title missing)")

                        # 描述
                        desc = ""
                        desc_tag = soup.find(
                            'div', class_='description-markdown-html')
                        if desc_tag:
                            desc = desc_tag.get_text(strip=True)

                        self.current_title = title
                        self.current_desc = desc[:100] + \
                            "..." if len(desc) > 100 else desc

                        item = {
                            'ID': target_id,
                            'URL': url,
                            'Title': title,
                            'Description': desc
                        }

                        # 更新数据
                        data_map[target_id] = item

                        existing_index = next((i for i, x in enumerate(
                            self.task_data['data']) if x['ID'] == target_id), -1)
                        if existing_index >= 0:
                            self.task_data['data'][existing_index] = item
                        else:
                            self.task_data['data'].append(item)

                        self.log(f"Fetched {target_id}: {title}")

                        if target_id in self.task_data['failed_ids']:
                            self.task_data['failed_ids'].remove(target_id)

                    finally:
                        page.close()

                except Exception as e:
                    err_msg = str(e)
                    self.log(f"Error {target_id}: {err_msg}")

                    if "ERR_INTERNET_DISCONNECTED" in err_msg or "Connection refused" in err_msg:
                        self.paused = True
                        self.log("Network error. Pausing.")
                        if is_custom:
                            self.task_data['custom_queue'].insert(0, target_id)
                        continue

                    if target_id not in self.task_data['failed_ids']:
                        self.task_data['failed_ids'].append(target_id)

                # 如果是顺序抓取，更新ID
                if not is_custom:
                    # 如果未到达终点，移动到下一个
                    if self.task_data['current_id'] < end_id:
                        self.task_data['current_id'] += 1
                    else:
                        # 刚刚完成了最后一项 (current_id == end_id)
                        self.task_data['status'] = 'completed'
                        self.running = False
                        self.log("Task completed.")
                        # 强制立即保存
                        if self.save_callback:
                            self.save_callback(self.task_data)
                        break

                count_since_save += 1
                if count_since_save >= 5:
                    if self.save_callback:
                        self.save_callback(self.task_data)
                    count_since_save = 0

                delay = self.task_data.get('delay', 1.0)
                time.sleep(delay)

            browser.close()

        self.running = False

        # 最终状态检查（以防循环以其他方式退出）
        if self.paused:
            self.task_data['status'] = 'paused'
        elif self.task_data['status'] != 'completed':
            self.task_data['status'] = 'stopped'

        if self.save_callback:
            self.save_callback(self.task_data)
        self.log("Crawler stopped.")
