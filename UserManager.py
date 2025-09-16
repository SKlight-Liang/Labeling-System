import sqlite3
import json
import os
from datetime import datetime, timedelta
from threading import Lock
from typing import Dict, List, Tuple, Optional, Any

class UserManager:
    """用户管理类，处理用户相关的数据库操作和统计"""
    
    def __init__(self, db_path: str = "users.db", db_lock: Optional[Lock] = None):
        """
        初始化用户管理器
        
        Args:
            db_path: 数据库文件路径
            db_lock: 数据库锁对象
        """
        self.db_path = db_path
        self.db_lock = db_lock or Lock()
    
    def get_user_detail_info(self, user_id: int) -> Optional[Dict[str, Any]]:
        """
        获取用户详细信息和统计数据
        
        Args:
            user_id: 用户ID
            
        Returns:
            Dict: 包含用户信息、统计数据和最近工作记录的字典，失败返回None
        """
        try:
            with self.db_lock:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                
                # 获取用户基本信息
                user_data = self._get_user_basic_info(cursor, user_id)
                if not user_data:
                    conn.close()
                    return None
                
                username = user_data[1]
                
                # 获取标注统计
                annotation_stats = self._get_annotation_statistics(cursor, username)
                
                # 获取最近标注记录（修复显示问题）
                recent_work = self._get_recent_annotation_work(cursor, username)
                
                # 获取用户警报记录
                user_alerts = self._get_user_alerts(username)
                
                # 计算活跃度和一致性
                activity_stats = self._calculate_activity_stats(cursor, user_data, annotation_stats['total_annotations'])
                consistency_stats = self._calculate_consistency_stats(cursor, username)
                time_distribution = self._get_time_distribution(cursor, username)
                
                conn.close()
                
                # 组装用户信息
                user_info = self._build_user_info(user_data)
                
                # 组装完整统计信息
                complete_stats = {
                    **annotation_stats,
                    **activity_stats,
                    **consistency_stats,
                    **time_distribution
                }
                
                return {
                    'user': user_info,
                    'stats': complete_stats,
                    'recent_work': recent_work,
                    'user_alerts': user_alerts[:10]  # 最近10条警报
                }
                
        except Exception as e:
            print(f"获取用户详情失败: {e}")
            return None
    
    def _get_user_basic_info(self, cursor, user_id: int) -> Optional[Tuple]:
        """获取用户基本信息"""
        cursor.execute('''
            SELECT id, username, created_at, last_active, last_login_ip, 
                   is_blocked, blocked_reason, blocked_at, blocked_by, annotation_reset_time
            FROM users WHERE id = ?
        ''', (user_id,))
        return cursor.fetchone()
    
    def _get_annotation_statistics(self, cursor, username: str) -> Dict[str, int]:
        """获取用户标注统计信息"""
        # 总标注数
        cursor.execute('''
            SELECT COUNT(*) FROM qa_data 
            WHERE first_annotator = ? OR second_annotator = ?
        ''', (username, username))
        total_annotations = cursor.fetchone()[0]
        
        # 作为第一标注者的数量
        cursor.execute('SELECT COUNT(*) FROM qa_data WHERE first_annotator = ?', (username,))
        first_annotations = cursor.fetchone()[0]
        
        # 作为第二标注者的数量
        cursor.execute('SELECT COUNT(*) FROM qa_data WHERE second_annotator = ?', (username,))
        second_annotations = cursor.fetchone()[0]
        
        # 最近7天的标注数量
        seven_days_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute('''
            SELECT COUNT(*) FROM qa_data 
            WHERE (first_annotator = ? AND first_annotation_time >= ?) 
               OR (second_annotator = ? AND second_annotation_time >= ?)
        ''', (username, seven_days_ago, username, seven_days_ago))
        recent_annotations = cursor.fetchone()[0]
        
        # 各类标注结果统计
        cursor.execute('''
            SELECT 
                SUM(CASE WHEN first_annotation_result = 'good' THEN 1 ELSE 0 END) +
                SUM(CASE WHEN second_annotation_result = 'good' THEN 1 ELSE 0 END) as good_count,
                SUM(CASE WHEN first_annotation_result = 'bad' THEN 1 ELSE 0 END) +
                SUM(CASE WHEN second_annotation_result = 'bad' THEN 1 ELSE 0 END) as bad_count,
                SUM(CASE WHEN first_annotation_result = 'uncertain' THEN 1 ELSE 0 END) +
                SUM(CASE WHEN second_annotation_result = 'uncertain' THEN 1 ELSE 0 END) as uncertain_count
            FROM qa_data 
            WHERE first_annotator = ? OR second_annotator = ?
        ''', (username, username))
        annotation_result_stats = cursor.fetchone()
        
        return {
            'total_annotations': total_annotations,
            'first_annotations': first_annotations,
            'second_annotations': second_annotations,
            'recent_annotations': recent_annotations,
            'good_count': annotation_result_stats[0] or 0,
            'bad_count': annotation_result_stats[1] or 0,
            'uncertain_count': annotation_result_stats[2] or 0
        }
    
    def _get_recent_annotation_work(self, cursor, username: str) -> List[Dict[str, Any]]:
        """获取最近的标注记录（修复显示问题）"""
        # 修改查询以获取更完整的信息并正确处理排序
        cursor.execute('''
            SELECT 
                id, 
                COALESCE(question_cn, question_en, '无问题') as question,
                CASE 
                    WHEN first_annotator = ? THEN first_annotation_result
                    WHEN second_annotator = ? THEN second_annotation_result
                    ELSE ''
                END as user_annotation_result,
                CASE 
                    WHEN first_annotator = ? THEN first_annotation_time
                    WHEN second_annotator = ? THEN second_annotation_time
                    ELSE ''
                END as user_annotation_time,
                CASE 
                    WHEN first_annotator = ? THEN 'first'
                    WHEN second_annotator = ? THEN 'second'
                    ELSE 'unknown'
                END as annotation_role,
                final_status,
                subject
            FROM qa_data 
            WHERE first_annotator = ? OR second_annotator = ?
            ORDER BY 
                CASE 
                    WHEN first_annotator = ? AND first_annotation_time != '' THEN first_annotation_time
                    WHEN second_annotator = ? AND second_annotation_time != '' THEN second_annotation_time
                    ELSE '1970-01-01 00:00:00'
                END DESC
            LIMIT 15
        ''', (username, username, username, username, username, username, username, username, username, username))
        
        recent_work_raw = cursor.fetchall()
        
        # 处理查询结果，转换为更易于模板使用的格式
        recent_work = []
        for work in recent_work_raw:
            if work and work[1]:  # 确保有问题内容
                work_dict = {
                    'id': work[0],
                    'question': work[1][:100] + '...' if len(work[1]) > 100 else work[1],
                    'user_annotation_result': work[2],
                    'user_annotation_time': work[3],
                    'annotation_role': work[4],
                    'final_status': work[5] or 'pending',
                    'subject': work[6] or '未分类',
                    'result_display': self._get_result_display(work[2]),
                    'role_display': '第一次标注' if work[4] == 'first' else '第二次标注' if work[4] == 'second' else '未知'
                }
                recent_work.append(work_dict)
        
        return recent_work
    
    def _get_result_display(self, result: str) -> str:
        """获取标注结果的显示文本"""
        result_mapping = {
            'good': 'Good',
            'bad': 'Bad', 
            'uncertain': 'Uncertain',
            '': 'Unlabeled'
        }
        return result_mapping.get(result, '未知')
    
    def _get_user_alerts(self, username: str) -> List[Dict[str, Any]]:
        """获取用户警报记录"""
        try:
            # 导入警报获取函数（避免循环导入）
            from app import get_annotation_alerts
            alerts = get_annotation_alerts(limit=50)
            return [alert for alert in alerts if alert['username'] == username]
        except Exception as e:
            print(f"获取警报记录失败: {e}")
            return []
    
    def _calculate_activity_stats(self, cursor, user_data: Tuple, total_annotations: int) -> Dict[str, float]:
        """计算用户活跃度统计"""
        avg_annotations_per_day = 0.0
        
        if user_data[2]:  # 如果有创建时间
            try:
                created_date = datetime.strptime(user_data[2], '%Y-%m-%d %H:%M:%S')
                days_since_creation = (datetime.now() - created_date).days + 1
                avg_annotations_per_day = total_annotations / days_since_creation if days_since_creation > 0 else 0
            except Exception as e:
                print(f"计算活跃度失败: {e}")
                avg_annotations_per_day = 0
        
        return {
            'avg_annotations_per_day': round(avg_annotations_per_day, 1)
        }
    
    def _calculate_consistency_stats(self, cursor, username: str) -> Dict[str, float]:
        """计算标注一致性统计"""
        cursor.execute('''
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN first_annotation_result = second_annotation_result THEN 1 ELSE 0 END) as consistent
            FROM qa_data 
            WHERE (first_annotator = ? AND second_annotator != '' AND second_annotator IS NOT NULL)
               OR (second_annotator = ? AND first_annotator != '' AND first_annotator IS NOT NULL)
        ''', (username, username))
        consistency_data = cursor.fetchone()
        
        consistency_rate = 0.0
        if consistency_data and consistency_data[0] > 0:
            consistency_rate = (consistency_data[1] / consistency_data[0]) * 100
        
        return {
            'consistency_rate': round(consistency_rate, 1)
        }
    
    def _get_time_distribution(self, cursor, username: str) -> Dict[str, Any]:
        """获取标注时间分布统计"""
        cursor.execute('''
            SELECT 
                SUBSTR(first_annotation_time, 12, 2) as hour,
                COUNT(*) as count
            FROM qa_data 
            WHERE first_annotator = ? AND first_annotation_time != ''
            GROUP BY hour
            UNION ALL
            SELECT 
                SUBSTR(second_annotation_time, 12, 2) as hour,
                COUNT(*) as count
            FROM qa_data 
            WHERE second_annotator = ? AND second_annotation_time != ''
            GROUP BY hour
        ''', (username, username))
        hour_data = cursor.fetchall()
        
        # 处理时间分布数据
        hour_stats = {}
        for hour, count in hour_data:
            if hour and hour.isdigit():
                hour_int = int(hour)
                hour_stats[hour_int] = hour_stats.get(hour_int, 0) + count
        
        most_active_hours = sorted(hour_stats.items(), key=lambda x: x[1], reverse=True)[:3] if hour_stats else []
        
        return {
            'hour_stats': hour_stats,
            'most_active_hours': most_active_hours
        }
    
    def _build_user_info(self, user_data: Tuple) -> Dict[str, Any]:
        """构建用户信息字典"""
        return {
            'id': user_data[0],
            'username': user_data[1],
            'created_at': user_data[2],
            'last_active': user_data[3],
            'last_login_ip': user_data[4] or '未知',
            'is_blocked': bool(user_data[5]),
            'blocked_reason': user_data[6] or '',
            'blocked_at': user_data[7] or '',
            'blocked_by': user_data[8] or '',
            'annotation_reset_time': user_data[9] or '',
            'online': self._is_user_online(user_data[3]) if user_data[3] else False
        }
    
    def _is_user_online(self, last_active_str: str) -> bool:
        """判断用户是否在线（10分钟内活跃）"""
        try:
            last_active = datetime.strptime(last_active_str, '%Y-%m-%d %H:%M:%S')
            current_time = datetime.now()
            time_diff = (current_time - last_active).total_seconds()
            return time_diff <= 600  # 10分钟 = 600秒
        except Exception:
            return False
    
    def get_user_basic_by_id(self, user_id: int) -> Optional[Tuple]:
        """根据ID获取用户基本信息（用于其他功能）"""
        try:
            with self.db_lock:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                cursor.execute('SELECT id, username, created_at, last_active, last_login_ip FROM users WHERE id = ?', (user_id,))
                user = cursor.fetchone()
                conn.close()
                return user
        except Exception as e:
            print(f"获取用户基本信息失败: {e}")
            return None
