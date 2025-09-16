import sqlite3
import time
import threading
from datetime import datetime
from threading import Lock, RLock


class DataDistributor:
    """数据分发器类 - 管理标注数据的分发和分配"""
    
    def __init__(self, db_path='users.db'):
        """
        初始化数据分发器
        
        Args:
            db_path (str): 数据库文件路径
        """
        self.db_path = db_path
        
        # 数据库锁，用于多线程安全
        self.db_lock = Lock()
        
        # 标注缓冲池管理
        self.annotation_buffer_lock = RLock()  # 可重入锁，防止死锁
        self.annotation_buffer = {
            'allocated': {},  # 已分配的数据：{data_id: {'username': username, 'timestamp': timestamp, 'step': 'first'|'second'}}
            'processing': set(),  # 正在处理的数据ID集合
        }
        
        # 缓冲池配置
        self.buffer_config = {
            'timeout': 120,  # 数据分配超时时间：2分钟（120秒）
            'cleanup_interval': 10,  # 清理间隔：1分钟
            'max_retry_attempts': 3,  # 最大重试次数
        }
        
        # 清理定时器
        self.cleanup_timer = None
        
    def start_cleanup_timer(self):
        """启动缓冲池清理定时器"""
        def cleanup_task():
            try:
                self.cleanup_expired_allocations()
            except Exception as e:
                print(f"缓冲池清理出错: {e}")
            finally:
                # 重新启动定时器
                if self.cleanup_timer:
                    self.cleanup_timer = threading.Timer(
                        self.buffer_config['cleanup_interval'], 
                        cleanup_task
                    )
                    self.cleanup_timer.daemon = True
                    self.cleanup_timer.start()
        
        # 首次启动定时器
        self.cleanup_timer = threading.Timer(
            self.buffer_config['cleanup_interval'], 
            cleanup_task
        )
        self.cleanup_timer.daemon = True
        self.cleanup_timer.start()
        print(f"缓冲池清理定时器已启动，清理间隔: {self.buffer_config['cleanup_interval']}秒")
    
    def stop_cleanup_timer(self):
        """停止缓冲池清理定时器"""
        if self.cleanup_timer:
            self.cleanup_timer.cancel()
            self.cleanup_timer = None
            print("缓冲池清理定时器已停止")
    
    def cleanup_expired_allocations(self):
        """清理过期的数据分配"""
        with self.annotation_buffer_lock:
            current_time = time.time()
            expired_data_ids = []
            
            for data_id, allocation in self.annotation_buffer['allocated'].items():
                if current_time - allocation['timestamp'] > self.buffer_config['timeout']:
                    expired_data_ids.append(data_id)
            
            # 清理过期分配
            for data_id in expired_data_ids:
                del self.annotation_buffer['allocated'][data_id]
                self.annotation_buffer['processing'].discard(data_id)
                print(f"清理过期分配: 数据ID {data_id}")
            
            return len(expired_data_ids)
    
    def allocate_data_for_annotation(self, data_id, username, step):
        """
        为用户分配标注数据
        
        Args:
            data_id (int): 数据ID
            username (str): 用户名
            step (str): 标注步骤 ('first' 或 'second')
            
        Returns:
            bool: 分配成功返回True，失败返回False
        """
        with self.annotation_buffer_lock:
            current_time = time.time()
            
            # 检查数据是否已被分配
            if data_id in self.annotation_buffer['allocated']:
                existing_allocation = self.annotation_buffer['allocated'][data_id]
                # 如果是同一个用户重新请求，更新时间戳
                if existing_allocation['username'] == username:
                    existing_allocation['timestamp'] = current_time
                    return True
                # 如果是不同用户，检查是否过期
                elif current_time - existing_allocation['timestamp'] > self.buffer_config['timeout']:
                    del self.annotation_buffer['allocated'][data_id]
                    self.annotation_buffer['processing'].discard(data_id)
                else:
                    return False  # 数据已被其他用户分配且未过期
            
            # 分配数据给用户
            self.annotation_buffer['allocated'][data_id] = {
                'username': username,
                'timestamp': current_time,
                'step': step
            }
            self.annotation_buffer['processing'].add(data_id)
            return True
    
    def release_data_allocation(self, data_id, username):
        """
        释放用户的数据分配
        
        Args:
            data_id (int): 数据ID
            username (str): 用户名
            
        Returns:
            bool: 释放成功返回True，失败返回False
        """
        with self.annotation_buffer_lock:
            if data_id in self.annotation_buffer['allocated']:
                allocation = self.annotation_buffer['allocated'][data_id]
                if allocation['username'] == username:
                    del self.annotation_buffer['allocated'][data_id]
                    self.annotation_buffer['processing'].discard(data_id)
                    return True
            return False
    
    def is_data_available_for_annotation(self, data_id, username, step=None):
        """
        检查数据是否可用于标注
        
        Args:
            data_id (int): 数据ID
            username (str): 用户名
            step (str, optional): 标注步骤
            
        Returns:
            bool: 可用返回True，不可用返回False
        """
        with self.annotation_buffer_lock:
            # 先清理过期分配
            self.cleanup_expired_allocations()
            
            if data_id in self.annotation_buffer['allocated']:
                allocation = self.annotation_buffer['allocated'][data_id]
                # 如果是同一用户，允许继续
                if allocation['username'] == username:
                    return True
                # 如果是不同用户，不允许
                else:
                    return False
            
            # 数据未被分配，可以使用
            return True
    
    def get_buffer_status(self):
        """
        获取缓冲池状态 - 仅供管理员查看
        
        Returns:
            dict: 缓冲池状态信息
        """
        with self.annotation_buffer_lock:
            self.cleanup_expired_allocations()
            return {
                'allocated_count': len(self.annotation_buffer['allocated']),
                'processing_count': len(self.annotation_buffer['processing']),
                'allocations': dict(self.annotation_buffer['allocated']),
                'config': self.buffer_config.copy()
            }
    
    def get_qa_data_for_annotation(self, username, limit=3):
        """
        获取待标注的问答数据 - 支持双人标注模式和缓冲池管理
        
        Args:
            username (str): 用户名
            limit (int): 获取数据的数量限制
            
        Returns:
            tuple or None: 数据记录元组，如果没有可用数据返回None
        """
        with self.annotation_buffer_lock:
            # 清理过期的分配
            self.cleanup_expired_allocations()
            
            with self.db_lock:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                
                # 优先获取待第一次标注的数据（pending状态）
                # 排除已经在缓冲池中被分配的数据
                allocated_ids = list(self.annotation_buffer['allocated'].keys())
                
                if allocated_ids:
                    placeholders = ','.join(['?' for _ in allocated_ids])
                    cursor.execute(f'''
                        SELECT id, question_cn, answer_cn, question_en, answer_en, 
                               subject, clue_urls, traces, answer_clue, answer_url,
                               annotation_status, first_annotator, second_annotator
                        FROM qa_data 
                        WHERE annotation_status = 'pending'
                           AND (first_annotator = '' OR first_annotator IS NULL)
                           AND id NOT IN ({placeholders})
                        ORDER BY created_at ASC 
                        LIMIT ?
                    ''', allocated_ids + [limit])
                else:
                    cursor.execute('''
                        SELECT id, question_cn, answer_cn, question_en, answer_en, 
                               subject, clue_urls, traces, answer_clue, answer_url,
                               annotation_status, first_annotator, second_annotator
                        FROM qa_data 
                        WHERE annotation_status = 'pending'
                           AND (first_annotator = '' OR first_annotator IS NULL)
                        ORDER BY created_at ASC 
                        LIMIT ?
                    ''', (limit,))
                
                data = cursor.fetchone()
                
                # 如果找到数据，尝试分配给用户
                if data:
                    data_id = data[0]
                    if self.allocate_data_for_annotation(data_id, username, 'first'):
                        conn.close()
                        return data
                    else:
                        # 分配失败，尝试下一条数据
                        data = None
                
                # 如果没有待第一次标注的数据，寻找待第二次标注的数据
                if not data:
                    if allocated_ids:
                        cursor.execute(f'''
                            SELECT id, question_cn, answer_cn, question_en, answer_en, 
                                   subject, clue_urls, traces, answer_clue, answer_url,
                                   annotation_status, first_annotator, second_annotator
                            FROM qa_data 
                            WHERE annotation_status = 'first_completed'
                               AND (second_annotator = '' OR second_annotator IS NULL)
                               AND first_annotator != ?
                               AND id NOT IN ({placeholders})
                            ORDER BY first_annotation_time ASC 
                            LIMIT ?
                        ''', [username] + allocated_ids + [limit])
                    else:
                        cursor.execute('''
                            SELECT id, question_cn, answer_cn, question_en, answer_en, 
                                   subject, clue_urls, traces, answer_clue, answer_url,
                                   annotation_status, first_annotator, second_annotator
                            FROM qa_data 
                            WHERE annotation_status = 'first_completed'
                               AND (second_annotator = '' OR second_annotator IS NULL)
                               AND first_annotator != ?
                            ORDER BY first_annotation_time ASC 
                            LIMIT ?
                        ''', (username, limit))
                    
                    data = cursor.fetchone()
                    
                    # 如果找到数据，尝试分配给用户
                    if data:
                        data_id = data[0]
                        if self.allocate_data_for_annotation(data_id, username, 'second'):
                            conn.close()
                            return data
                
                conn.close()
                return None
    
    def save_annotation(self, data_id, username, annotation_result):
        """
        保存标注结果 - 支持双人标注模式和缓冲池管理
        
        Args:
            data_id (int): 数据ID
            username (str): 用户名
            annotation_result (str): 标注结果 ('good', 'bad', 'uncertain')
            
        Returns:
            bool: 保存成功返回True，失败返回False
        """
        # 首先检查用户是否有权限标注这个数据
        if not self.is_data_available_for_annotation(data_id, username, None):
            print(f"用户 {username} 没有权限标注数据 {data_id}")
            return False
        
        with self.db_lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # 获取当前数据的标注状态
            cursor.execute('''
                SELECT annotation_status, first_annotator, first_annotation_result,
                       second_annotator, second_annotation_result
                FROM qa_data 
                WHERE id = ?
            ''', (data_id,))
            current_data = cursor.fetchone()
            
            if not current_data:
                conn.close()
                return False
            
            status, first_annotator, first_result, second_annotator, second_result = current_data
            
            success = False
            
            # 如果是第一次标注
            if status == 'pending' and (not first_annotator or first_annotator == ''):
                cursor.execute('''
                    UPDATE qa_data 
                    SET annotation_status = 'first_completed',
                        first_annotator = ?,
                        first_annotation_result = ?,
                        first_annotation_time = ?
                    WHERE id = ?
                ''', (username, annotation_result, current_time, data_id))
                success = True
                
            # 如果是第二次标注
            elif status == 'first_completed' and first_annotator != username and (not second_annotator or second_annotator == ''):
                # 判断两次标注结果是否一致
                if annotation_result == first_result:
                    final_status = 'agreed'  # 一致
                else:
                    final_status = 'conflicted'  # 冲突
                
                cursor.execute('''
                    UPDATE qa_data 
                    SET annotation_status = 'completed',
                        second_annotator = ?,
                        second_annotation_result = ?,
                        second_annotation_time = ?,
                        final_status = ?
                    WHERE id = ?
                ''', (username, annotation_result, current_time, final_status, data_id))
                success = True
            else:
                # 不符合标注条件
                conn.close()
                return False
            
            if success:
                conn.commit()
                # 标注成功后，释放缓冲池中的分配
                self.release_data_allocation(data_id, username)
            
            conn.close()
            return success
    
    def get_user_annotation_stats(self, username):
        """
        获取用户标注统计 - 支持双人标注模式
        
        Args:
            username (str): 用户名
            
        Returns:
            dict: 用户标注统计信息
        """
        with self.db_lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 统计作为第一标注者的数据
            cursor.execute('''
                SELECT 
                    COUNT(*) as first_total,
                    SUM(CASE WHEN first_annotation_result = 'good' THEN 1 ELSE 0 END) as first_good,
                    SUM(CASE WHEN first_annotation_result = 'bad' THEN 1 ELSE 0 END) as first_bad,
                    SUM(CASE WHEN first_annotation_result = 'uncertain' THEN 1 ELSE 0 END) as first_uncertain
                FROM qa_data 
                WHERE first_annotator = ?
            ''', (username,))
            first_stats = cursor.fetchone()
            
            # 统计作为第二标注者的数据
            cursor.execute('''
                SELECT 
                    COUNT(*) as second_total,
                    SUM(CASE WHEN second_annotation_result = 'good' THEN 1 ELSE 0 END) as second_good,
                    SUM(CASE WHEN second_annotation_result = 'bad' THEN 1 ELSE 0 END) as second_bad,
                    SUM(CASE WHEN second_annotation_result = 'uncertain' THEN 1 ELSE 0 END) as second_uncertain
                FROM qa_data 
                WHERE second_annotator = ?
            ''', (username,))
            second_stats = cursor.fetchone()
            
            # 统计参与的一致和冲突数据
            cursor.execute('''
                SELECT 
                    SUM(CASE WHEN final_status = 'agreed' THEN 1 ELSE 0 END) as agreed_count,
                    SUM(CASE WHEN final_status = 'conflicted' THEN 1 ELSE 0 END) as conflict_count
                FROM qa_data 
                WHERE first_annotator = ? OR second_annotator = ?
            ''', (username, username))
            consistency_stats = cursor.fetchone()
            
            conn.close()
            
            # 计算统计结果
            first_total = first_stats[0] if first_stats[0] else 0
            second_total = second_stats[0] if second_stats[0] else 0
            total_annotations = first_total + second_total
            
            agreed_count = consistency_stats[0] if consistency_stats[0] else 0
            conflict_count = consistency_stats[1] if consistency_stats[1] else 0
            
            # 计算一致性率
            if agreed_count + conflict_count > 0:
                consistency_rate = round((agreed_count / (agreed_count + conflict_count)) * 100, 1)
            else:
                consistency_rate = 0
            
            return {
                'total_annotations': total_annotations,
                'first_annotations': first_total,
                'second_annotations': second_total,
                'first_good': first_stats[1] if first_stats[1] else 0,
                'first_bad': first_stats[2] if first_stats[2] else 0,
                'first_uncertain': first_stats[3] if first_stats[3] else 0,
                'second_good': second_stats[1] if second_stats[1] else 0,
                'second_bad': second_stats[2] if second_stats[2] else 0,
                'second_uncertain': second_stats[3] if second_stats[3] else 0,
                'agreed_count': agreed_count,
                'conflict_count': conflict_count,
                'consistency_rate': consistency_rate
            }
    
    def force_release_user_data(self, username):
        """
        强制释放用户的所有数据分配（管理员功能）
        
        Args:
            username (str): 用户名
            
        Returns:
            int: 释放的数据数量
        """
        with self.annotation_buffer_lock:
            released_count = 0
            data_ids_to_remove = []
            
            for data_id, allocation in self.annotation_buffer['allocated'].items():
                if allocation['username'] == username:
                    data_ids_to_remove.append(data_id)
            
            for data_id in data_ids_to_remove:
                del self.annotation_buffer['allocated'][data_id]
                self.annotation_buffer['processing'].discard(data_id)
                released_count += 1
                print(f"强制释放用户 {username} 的数据分配: 数据ID {data_id}")
            
            return released_count
    
    def get_allocated_data_by_user(self, username):
        """
        获取用户当前分配的数据列表
        
        Args:
            username (str): 用户名
            
        Returns:
            list: 分配给用户的数据ID列表
        """
        with self.annotation_buffer_lock:
            self.cleanup_expired_allocations()
            allocated_data = []
            
            for data_id, allocation in self.annotation_buffer['allocated'].items():
                if allocation['username'] == username:
                    allocated_data.append({
                        'data_id': data_id,
                        'step': allocation['step'],
                        'timestamp': allocation['timestamp'],
                        'allocated_time': datetime.fromtimestamp(allocation['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
                    })
            
            return allocated_data
    
    def update_buffer_config(self, new_config):
        """
        更新缓冲池配置
        
        Args:
            new_config (dict): 新的配置字典
        """
        self.buffer_config.update(new_config)
        print(f"缓冲池配置已更新: {self.buffer_config}")
    
    def __del__(self):
        """析构函数 - 确保清理定时器被停止"""
        self.stop_cleanup_timer()
