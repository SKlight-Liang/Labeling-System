import sqlite3
import os
from typing import List, Dict, Any, Optional, Union

class DataManager:
    """数据管理器类"""
    
    def __init__(self, db_path: str = "users.db"):
        """
        初始化数据管理器
        
        Args:
            db_path: 数据库文件路径
        """
        self.db_path = db_path
        self.table_name = "qa_data"
    
    def _get_connection(self):
        """获取数据库连接"""
        if not os.path.exists(self.db_path):
            raise FileNotFoundError(f"数据库文件不存在: {self.db_path}")
        return sqlite3.connect(self.db_path)
    
    def delete_qa_data_by_ids(self, ids: Union[int, List[int]], delete_type: str = "batch") -> Dict[str, Any]:
        """
        根据ID删除QA数据
        
        Args:
            ids: 要删除的数据ID，可以是单个ID或ID列表
            delete_type: 删除类型，'single' 或 'batch'
        
        Returns:
            Dict: 包含删除结果的字典
                {
                    'success': bool,
                    'message': str,
                    'deleted_count': int,
                    'deleted_ids': List[int]
                }
        """
        try:
            # 处理输入参数
            if isinstance(ids, int):
                id_list = [ids]
            elif isinstance(ids, list):
                id_list = [int(id_val) for id_val in ids if str(id_val).strip()]
            else:
                return {
                    'success': False,
                    'message': '无效的ID格式',
                    'deleted_count': 0,
                    'deleted_ids': []
                }
            
            if not id_list:
                return {
                    'success': False,
                    'message': '没有要删除的数据',
                    'deleted_count': 0,
                    'deleted_ids': []
                }
            
            # 验证删除类型
            if delete_type not in ['single', 'batch']:
                return {
                    'success': False,
                    'message': '无效的删除类型',
                    'deleted_count': 0,
                    'deleted_ids': []
                }
            
            # 如果是单条删除但提供了多个ID，返回错误
            if delete_type == 'single' and len(id_list) > 1:
                return {
                    'success': False,
                    'message': '单条删除模式下只能删除一条数据',
                    'deleted_count': 0,
                    'deleted_ids': []
                }
            
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # 开始事务
            cursor.execute('BEGIN TRANSACTION')
            
            try:
                # 检查要删除的数据是否存在
                placeholders = ','.join(['?' for _ in id_list])
                check_query = f"SELECT id FROM {self.table_name} WHERE id IN ({placeholders})"
                cursor.execute(check_query, id_list)
                existing_ids = [row[0] for row in cursor.fetchall()]
                
                if not existing_ids:
                    cursor.execute('ROLLBACK')
                    conn.close()
                    return {
                        'success': False,
                        'message': '要删除的数据不存在',
                        'deleted_count': 0,
                        'deleted_ids': []
                    }
                
                # 构建删除查询
                delete_placeholders = ','.join(['?' for _ in existing_ids])
                delete_query = f"DELETE FROM {self.table_name} WHERE id IN ({delete_placeholders})"
                
                # 执行删除
                cursor.execute(delete_query, existing_ids)
                deleted_count = cursor.rowcount
                
                # 提交事务
                cursor.execute('COMMIT')
                conn.close()
                
                # 检查未找到的ID
                missing_ids = [id_val for id_val in id_list if id_val not in existing_ids]
                message = f'成功删除 {deleted_count} 条数据'
                if missing_ids:
                    message += f'，未找到的ID: {missing_ids}'
                
                return {
                    'success': True,
                    'message': message,
                    'deleted_count': deleted_count,
                    'deleted_ids': existing_ids
                }
                
            except Exception as e:
                # 回滚事务
                cursor.execute('ROLLBACK')
                conn.close()
                raise e
                
        except ValueError as e:
            return {
                'success': False,
                'message': f'无效的ID格式: {str(e)}',
                'deleted_count': 0,
                'deleted_ids': []
            }
        except Exception as e:
            return {
                'success': False,
                'message': f'删除失败: {str(e)}',
                'deleted_count': 0,
                'deleted_ids': []
            }
    
    def parse_delete_ids(self, ids_string: str, delete_type: str) -> List[int]:
        """
        解析删除ID字符串
        
        Args:
            ids_string: ID字符串，格式如 "1,2,3" 或 "1"
            delete_type: 删除类型
        
        Returns:
            List[int]: 解析后的ID列表
        """
        if not ids_string or not ids_string.strip():
            return []
        
        try:
            if delete_type == 'single':
                return [int(ids_string.strip())]
            elif delete_type == 'batch':
                return [int(id_str.strip()) for id_str in ids_string.split(',') if id_str.strip()]
            else:
                return []
        except ValueError:
            return []
    
    def clear_all_data(self, confirm_code: str = None) -> Dict[str, Any]:
        """
        清空所有QA数据
        
        Args:
            confirm_code: 确认码，必须为 'CLEAR ALL DATA'
        
        Returns:
            Dict: 包含清空结果的字典
                {
                    'success': bool,
                    'message': str,
                    'cleared_count': int
                }
        """
        try:
            # 验证确认码
            if confirm_code != 'CLEAR ALL DATA':
                return {
                    'success': False,
                    'message': '确认码错误，请输入 "CLEAR ALL DATA"',
                    'cleared_count': 0
                }
            
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # 开始事务
            cursor.execute('BEGIN TRANSACTION')
            
            try:
                # 获取清空前的数据统计
                cursor.execute(f"SELECT COUNT(*) FROM {self.table_name}")
                total_count = cursor.fetchone()[0]
                
                if total_count == 0:
                    cursor.execute('ROLLBACK')
                    conn.close()
                    return {
                        'success': True,
                        'message': '数据库已经是空的，无需清空',
                        'cleared_count': 0
                    }
                
                # 清空qa_data表
                cursor.execute(f"DELETE FROM {self.table_name}")
                
                # 重置自增主键（如果存在序列表）
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='sqlite_sequence'")
                if cursor.fetchone():
                    cursor.execute(f"DELETE FROM sqlite_sequence WHERE name='{self.table_name}'")
                
                # 提交事务
                cursor.execute('COMMIT')
                conn.close()
                
                return {
                    'success': True,
                    'message': f'数据库已成功清空！共删除 {total_count} 条数据',
                    'cleared_count': total_count
                }
                
            except Exception as e:
                # 如果出错，回滚事务
                cursor.execute('ROLLBACK')
                conn.close()
                raise e
                
        except Exception as e:
            return {
                'success': False,
                'message': f'清空失败: {str(e)}',
                'cleared_count': 0
            }
    
    def get_data_count(self) -> int:
        """
        获取数据总数
        
        Returns:
            int: 数据总数
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {self.table_name}")
            count = cursor.fetchone()[0]
            conn.close()
            return count
        except Exception:
            return 0
    
    def get_data_statistics(self) -> Dict[str, Any]:
        """
        获取数据统计信息
        
        Returns:
            Dict: 包含各种统计信息的字典
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            stats = {}
            
            # 总数据量
            cursor.execute(f"SELECT COUNT(*) FROM {self.table_name}")
            stats['total_count'] = cursor.fetchone()[0]
            
            # 按状态分组统计
            cursor.execute(f"""
                SELECT annotation_status, COUNT(*) 
                FROM {self.table_name} 
                GROUP BY annotation_status
            """)
            status_counts = dict(cursor.fetchall())
            stats['status_counts'] = status_counts
            
            # 按最终状态分组统计
            cursor.execute(f"""
                SELECT final_status, COUNT(*) 
                FROM {self.table_name} 
                WHERE final_status IS NOT NULL AND final_status != ''
                GROUP BY final_status
            """)
            final_status_counts = dict(cursor.fetchall())
            stats['final_status_counts'] = final_status_counts
            
            # 按上传者分组统计
            cursor.execute(f"""
                SELECT uploaded_by, COUNT(*) 
                FROM {self.table_name} 
                WHERE uploaded_by IS NOT NULL AND uploaded_by != ''
                GROUP BY uploaded_by
                ORDER BY COUNT(*) DESC
                LIMIT 10
            """)
            uploader_counts = dict(cursor.fetchall())
            stats['top_uploaders'] = uploader_counts
            
            conn.close()
            return stats
            
        except Exception as e:
            return {
                'error': f'获取统计信息失败: {str(e)}',
                'total_count': 0,
                'status_counts': {},
                'final_status_counts': {},
                'top_uploaders': {}
            }
    
    def backup_data_before_delete(self, output_file: Optional[str] = None) -> Dict[str, Any]:
        """
        在删除前备份数据
        
        Args:
            output_file: 备份文件路径，如果不提供则自动生成
        
        Returns:
            Dict: 备份结果
        """
        try:
            from datetime import datetime
            import json
            
            if not output_file:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_file = f"qa_data_backup_{timestamp}.json"
            
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # 获取所有数据
            cursor.execute(f"SELECT * FROM {self.table_name}")
            rows = cursor.fetchall()
            
            # 获取列名
            cursor.execute(f"PRAGMA table_info({self.table_name})")
            columns = [column[1] for column in cursor.fetchall()]
            
            conn.close()
            
            # 转换为字典格式
            data = []
            for row in rows:
                item = dict(zip(columns, row))
                data.append(item)
            
            # 保存备份文件
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            return {
                'success': True,
                'message': f'数据备份成功，共备份 {len(data)} 条数据',
                'backup_file': output_file,
                'backup_count': len(data)
            }
            
        except Exception as e:
            return {
                'success': False,
                'message': f'备份失败: {str(e)}',
                'backup_file': None,
                'backup_count': 0
            }