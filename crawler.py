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
        task_type = self.task_data.get('task_type')
        target_name = self.task_data.get('target_name')
        start_page = self.task_data.get('start_page')
        end_page = self.task_data.get('end_page')

        # 确保数据结构存在
        if 'data' not in self.task_data:
            self.task_data['data'] = []
        if 'failed_ids' not in self.task_data:
            self.task_data['failed_ids'] = []
        if 'failed_pages' not in self.task_data:
            self.task_data['failed_pages'] = []
        if 'custom_queue' not in self.task_data:
            self.task_data['custom_queue'] = []
        if 'discovered_ids' not in self.task_data:
            self.task_data['discovered_ids'] = []

        # 创建映射以便快速查找，避免重复
        data_map = {item['ID']: item for item in self.task_data['data']}
        discovered_set = set(self.task_data['discovered_ids'])

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            )
            context.set_default_timeout(30000)

            self.log(f"Started crawling task: {self.task_data.get('name')}")

            while self.running:
                if self.paused:
                    time.sleep(0.5)
                    continue

                # 1. 优先处理自定义队列（重试失败的游戏ID）
                if self.task_data['custom_queue']:
                    target_id = self.task_data['custom_queue'].pop(0)
                    self.processing_id = target_id
                    self._crawl_game(context, target_id,
                                     data_map, is_custom=True)

                    # 保存检查
                    count_since_save += 1
                    if count_since_save >= 5:
                        if self.save_callback:
                            self.save_callback(self.task_data)
                        count_since_save = 0
                    continue

                # 2. 正常流程：按页抓取
                current_page = self.task_data.get('current_page', start_page)

                if current_page > end_page:
                    self.log("Reached end of page range.")
                    self.running = False
                    self.task_data['status'] = 'completed'
                    break

                # 构建列表页 URL
                if task_type == 'series':
                    list_url = f"https://zaixianwan.app/series/{target_name}?page={current_page}"
                else:  # console
                    list_url = f"https://zaixianwan.app/consoles/{target_name}?page={current_page}"

                self.current_url = list_url
                self.log(f"Scanning Page {current_page}: {list_url}")

                try:
                    page_ids = self._scan_list_page(context, list_url)

                    if not page_ids:
                        self.log(
                            f"Page {current_page} returned no IDs. Stopping.")
                        # 如果页面为空，可能已经超出了实际页数
                        self.running = False
                        self.task_data['status'] = 'completed'
                        break

                    # 更新已发现ID列表
                    new_ids_count = 0
                    for pid in page_ids:
                        if pid not in discovered_set:
                            self.task_data['discovered_ids'].append(pid)
                            discovered_set.add(pid)
                            new_ids_count += 1

                    self.log(
                        f"Page {current_page}: Found {len(page_ids)} IDs ({new_ids_count} new).")

                    # 抓取本页的所有游戏
                    for gid in page_ids:
                        if not self.running:
                            break
                        if self.paused:
                            while self.paused:
                                time.sleep(0.5)

                        # 如果已经在数据中，跳过（除非强制刷新，这里默认跳过）
                        if gid in data_map:
                            continue

                        self.processing_id = gid
                        self._crawl_game(
                            context, gid, data_map, is_custom=False)

                        # 每次抓取后稍微延迟
                        delay = self.task_data.get('delay', 1.0)
                        time.sleep(delay)

                    # 页面完成
                    if current_page in self.task_data['failed_pages']:
                        self.task_data['failed_pages'].remove(current_page)

                    self.task_data['current_page'] += 1

                    # 保存进度
                    if self.save_callback:
                        self.save_callback(self.task_data)

                except Exception as e:
                    self.log(f"Error scanning page {current_page}: {e}")
                    if current_page not in self.task_data['failed_pages']:
                        self.task_data['failed_pages'].append(current_page)

                    # 遇到页面错误，暂停还是继续？
                    # 这里选择暂停，防止网络问题导致连续翻页失败
                    self.paused = True
                    self.log("Page scan failed. Pausing.")

            browser.close()

        self.running = False

        # 最终状态检查
        if self.paused:
            self.task_data['status'] = 'paused'
        elif self.task_data['status'] != 'completed':
            self.task_data['status'] = 'stopped'

        if self.save_callback:
            self.save_callback(self.task_data)
        self.log("Crawler stopped.")

    def _scan_list_page(self, context, url):
        import re
        page = context.new_page()
        try:
            # 使用 networkidle 确保动态内容已加载
            page.goto(url, timeout=30000, wait_until='networkidle')

            # 等待至少一个游戏链接出现
            try:
                page.wait_for_selector('a[href^="/games/"]', timeout=5000)
            except:
                self.log(f"Warning: No game links found immediately on {url}")

            # 提取所有 /games/xxxxx 链接
            # 使用 evaluate 执行 JS 提取更稳健
            links = page.eval_on_selector_all(
                'a[href^="/games/"]', 'elements => elements.map(e => e.getAttribute("href"))')

            ids = []
            for link in links:
                match = re.search(r'/games/(\d+)', link)
                if match:
                    ids.append(int(match.group(1)))

            # 去重并保持顺序
            return sorted(list(set(ids)))
        finally:
            page.close()

    def _crawl_game(self, context, target_id, data_map, is_custom=False):
        url = f"https://zaixianwan.app/games/{target_id}"
        self.current_url = url

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

                # 等待元素
                try:
                    page.wait_for_selector('.game-title', timeout=3000)
                except:
                    pass
                try:
                    page.wait_for_selector(
                        '.description-markdown-html', timeout=2000)
                except:
                    pass

                content = page.content()
                soup = BeautifulSoup(content, 'html.parser')

                # 提取数据
                title = ""
                title_tag = soup.find('span', class_='game-title')
                if title_tag:
                    title = title_tag.get_text(strip=True)

                if not title:
                    raise Exception("Title not found")

                desc = ""
                desc_tag = soup.find('div', class_='description-markdown-html')
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

                # 更新列表中的数据
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
                return

            if target_id not in self.task_data['failed_ids']:
                self.task_data['failed_ids'].append(target_id)
