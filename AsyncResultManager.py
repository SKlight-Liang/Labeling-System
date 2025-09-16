import time
import threading
from collections import defaultdict, deque
from threading import Lock, RLock
from datetime import datetime, timedelta


class AsyncResultManager:
    """异步处理结果管理器"""
    
    def __init__(self):
        """初始化异步结果管理器"""
        # 存储异步处理结果
        self.annotation_results = {}
        self.annotation_results_lock = RLock()
        
        # 请求频率限制
        self.user_request_history = defaultdict(deque)  # 用户请求历史
        self.request_limit_lock = Lock()
        
        # 配置参数
        self.config = {
            'result_expire_time': 300,      # 结果过期时间（秒）
            'max_requests_per_minute': 30,  # 每分钟最大请求数
            'request_window_size': 60,      # 请求窗口大小（秒）
            'cache_expire_time': 10,        # 缓存过期时间（秒）
        }
        
        # 结果缓存
        self.result_cache = {}
        self.cache_lock = Lock()
        
        # 统计信息
        self.stats = {
            'total_requests': 0,
            'blocked_requests': 0,
            'cache_hits': 0,
            'cache_misses': 0
        }
    
    def check_request_rate_limit(self, user_id, client_ip=None):
        """
        检查用户请求频率限制
        
        Args:
            user_id (str): 用户ID
            client_ip (str, optional): 客户端IP
            
        Returns:
            tuple: (is_allowed, remaining_requests, reset_time)
        """
        current_time = time.time()
        window_start = current_time - self.config['request_window_size']
        
        with self.request_limit_lock:
            user_requests = self.user_request_history[user_id]
            
            # 清理过期请求记录
            while user_requests and user_requests[0] < window_start:
                user_requests.popleft()
            
            # 检查是否超过限制
            if len(user_requests) >= self.config['max_requests_per_minute']:
                self.stats['blocked_requests'] += 1
                oldest_request = user_requests[0]
                reset_time = oldest_request + self.config['request_window_size']
                remaining_requests = 0
                return False, remaining_requests, reset_time
            
            # 记录当前请求
            user_requests.append(current_time)
            self.stats['total_requests'] += 1
            
            remaining_requests = self.config['max_requests_per_minute'] - len(user_requests)
            reset_time = current_time + self.config['request_window_size']
            
            return True, remaining_requests, reset_time
    
    def get_cached_result(self, task_id):
        """
        获取缓存的结果
        
        Args:
            task_id (str): 任务ID
            
        Returns:
            dict or None: 缓存的结果，如果不存在或过期返回None
        """
        current_time = time.time()
        
        with self.cache_lock:
            if task_id in self.result_cache:
                cache_entry = self.result_cache[task_id]
                
                # 检查缓存是否过期
                if current_time - cache_entry['timestamp'] < self.config['cache_expire_time']:
                    self.stats['cache_hits'] += 1
                    return cache_entry['result']
                else:
                    # 删除过期缓存
                    del self.result_cache[task_id]
        
        self.stats['cache_misses'] += 1
        return None
    
    def set_cached_result(self, task_id, result):
        """
        设置缓存结果
        
        Args:
            task_id (str): 任务ID
            result (dict): 结果数据
        """
        current_time = time.time()
        
        with self.cache_lock:
            self.result_cache[task_id] = {
                'result': result.copy(),
                'timestamp': current_time
            }
    
    def store_result(self, task_id, username, status, result=None, data_id=None):
        """
        存储异步处理结果
        
        Args:
            task_id (str): 任务ID
            username (str): 用户名
            status (str): 状态 ('pending', 'completed', 'failed')
            result (dict, optional): 结果数据
            data_id (int, optional): 数据ID
        """
        current_time = time.time()
        
        with self.annotation_results_lock:
            self.annotation_results[task_id] = {
                'username': username,
                'status': status,
                'result': result,
                'data_id': data_id,
                'timestamp': current_time,
                'created_at': current_time
            }
            
        # 🎯 如果任务已完成或失败，立即设置内部缓存
        if status in ['completed', 'failed']:
            response_data = {
                'success': True,
                'task_id': task_id,
                'status': status,
                'result': result,
                'timestamp': current_time
            }
            self.set_cached_result(task_id, response_data)
            print(f"🎯 任务{status}立即缓存: {task_id} (AsyncResultManager)")
    
    def get_result(self, task_id, username=None):
        """
        获取异步处理结果
        
        Args:
            task_id (str): 任务ID
            username (str, optional): 用户名（用于权限检查）
            
        Returns:
            dict: 结果数据或错误信息
        """
        # 先检查缓存
        cached_result = self.get_cached_result(task_id)
        if cached_result:
            return cached_result
        
        with self.annotation_results_lock:
            if task_id not in self.annotation_results:
                return {
                    'success': False,
                    'message': '任务不存在或已过期',
                    'status': 'not_found'
                }
            
            result_data = self.annotation_results[task_id]
            
            # 检查权限
            if username and result_data['username'] != username:
                return {
                    'success': False,
                    'message': '无权查看此任务结果',
                    'status': 'forbidden'
                }
            
            response = {
                'success': True,
                'task_id': task_id,
                'status': result_data['status'],
                'result': result_data['result'],
                'timestamp': result_data['timestamp']
            }
            
            # 缓存结果
            self.set_cached_result(task_id, response)
            
            return response
    
    def cleanup_expired_results(self):
        """清理过期的结果"""
        current_time = time.time()
        expired_tasks = []
        
        with self.annotation_results_lock:
            for task_id, result_data in self.annotation_results.items():
                if current_time - result_data['timestamp'] > self.config['result_expire_time']:
                    expired_tasks.append(task_id)
            
            for task_id in expired_tasks:
                del self.annotation_results[task_id]
        
        # 清理过期缓存
        with self.cache_lock:
            expired_cache_keys = []
            for task_id, cache_entry in self.result_cache.items():
                if current_time - cache_entry['timestamp'] > self.config['cache_expire_time']:
                    expired_cache_keys.append(task_id)
            
            for task_id in expired_cache_keys:
                del self.result_cache[task_id]
        
        return len(expired_tasks)
    
    def get_stats(self):
        """获取统计信息"""
        with self.annotation_results_lock:
            active_results = len(self.annotation_results)
        
        with self.cache_lock:
            cached_results = len(self.result_cache)
        
        return {
            'active_results': active_results,
            'cached_results': cached_results,
            'total_requests': self.stats['total_requests'],
            'blocked_requests': self.stats['blocked_requests'],
            'cache_hits': self.stats['cache_hits'],
            'cache_misses': self.stats['cache_misses'],
            'cache_hit_rate': self.stats['cache_hits'] / max(1, self.stats['cache_hits'] + self.stats['cache_misses']) * 100,
            'config': self.config.copy()
        }
    
    def update_config(self, new_config):
        """更新配置"""
        self.config.update(new_config)
    
    def reset_stats(self):
        """重置统计信息"""
        self.stats = {
            'total_requests': 0,
            'blocked_requests': 0,
            'cache_hits': 0,
            'cache_misses': 0
        }


# 创建全局实例
async_result_manager = AsyncResultManager()
