#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据上传库 (Data Uploader Library)
用于处理QA数据的上传和批量导入操作

主要功能:
1. JSON数据验证和解析
2. 批量数据插入到数据库
3. 文件上传处理
4. 数据格式转换和验证
5. 上传统计和错误处理
"""

import json
import sqlite3
import os
from datetime import datetime
from typing import List, Dict, Any, Optional, Union, Tuple
from threading import Lock


class DataUploader:
    """数据上传器类"""
    
    def __init__(self, db_path: str = "users.db", db_lock: Optional[Lock] = None):
        """
        初始化数据上传器
        
        Args:
            db_path: 数据库文件路径
            db_lock: 数据库锁（可选，用于多线程安全）
        """
        self.db_path = db_path
        self.db_lock = db_lock or Lock()
        self.table_name = "qa_data"
        
        # 支持的字段映射
        self.field_mapping = {
            '问题': 'question_cn',
            '答案': 'answer_cn', 
            '问题（英语）': 'question_en',
            '答案（英语）': 'answer_en',
            '问题学科': 'subject',
            'clue_urls': 'clue_urls',
            'traces': 'traces',
            'answer_clue': 'answer_clue',
            'answer_url': 'answer_url',
            'check_info': 'check_info',
            'dfsw': 'dfsw_info'
        }
        
        # 必需字段
        self.required_fields = ['问题', '答案']
        
        # JSON字段（需要序列化存储的字段）
        self.json_fields = ['clue_urls', 'traces', 'check_info', 'dfsw']
    
    def _get_connection(self):
        """获取数据库连接"""
        if not os.path.exists(self.db_path):
            raise FileNotFoundError(f"数据库文件不存在: {self.db_path}")
        return sqlite3.connect(self.db_path)
    
    def validate_data_format(self, data: Any) -> Tuple[bool, str]:
        """
        验证数据格式
        
        Args:
            data: 要验证的数据
        
        Returns:
            Tuple[bool, str]: (是否有效, 错误信息)
        """
        try:
            # 检查是否为列表
            if not isinstance(data, list):
                return False, "数据格式错误：应该是一个数组"
            
            if len(data) == 0:
                return False, "数据为空：至少需要一条数据"
            
            # 检查每条数据的格式
            for i, item in enumerate(data):
                if not isinstance(item, dict):
                    return False, f"第{i+1}条数据格式错误：应该是一个对象"
                
                # 检查必需字段
                for required_field in self.required_fields:
                    if required_field not in item or not item[required_field]:
                        return False, f"第{i+1}条数据缺少必需字段: {required_field}"
            
            return True, ""
            
        except Exception as e:
            return False, f"数据验证异常: {str(e)}"
    
    def parse_json_data(self, data_text: str) -> Tuple[bool, Union[List[Dict], str]]:
        """
        解析JSON数据
        
        Args:
            data_text: JSON字符串
        
        Returns:
            Tuple[bool, Union[List[Dict], str]]: (是否成功, 数据或错误信息)
        """
        try:
            if not data_text or not data_text.strip():
                return False, "JSON数据不能为空"
            
            # 解析JSON
            data = json.loads(data_text.strip())
            
            # 验证数据格式
            is_valid, error_msg = self.validate_data_format(data)
            if not is_valid:
                return False, error_msg
            
            return True, data
            
        except json.JSONDecodeError as e:
            return False, f"JSON格式错误: {str(e)}"
        except Exception as e:
            return False, f"解析错误: {str(e)}"
    
    def process_file_upload(self, file_obj) -> Tuple[bool, Union[str, str]]:
        """
        处理文件上传
        
        Args:
            file_obj: 上传的文件对象
        
        Returns:
            Tuple[bool, Union[str, str]]: (是否成功, 文件内容或错误信息)
        """
        try:
            if not file_obj or not file_obj.filename:
                return False, "没有选择文件"
            
            # 检查文件扩展名
            if not file_obj.filename.lower().endswith('.json'):
                return False, "请上传.json格式的文件"
            
            # 读取文件内容
            try:
                content = file_obj.read().decode('utf-8')
                return True, content
            except UnicodeDecodeError:
                return False, "文件编码错误，请确保文件为UTF-8编码"
            
        except Exception as e:
            return False, f"文件处理失败: {str(e)}"
    
    def prepare_data_for_insert(self, data: List[Dict], uploaded_by: str) -> List[Tuple]:
        """
        准备数据用于插入数据库
        
        Args:
            data: 数据列表
            uploaded_by: 上传者
        
        Returns:
            List[Tuple]: 准备好的数据元组列表
        """
        prepared_data = []
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        for item in data:
            # 准备数据元组
            data_tuple = (
                item.get('问题'),
                item.get('答案'),
                item.get('问题（英语）'),
                item.get('答案（英语）'),
                item.get('问题学科'),
                json.dumps(item.get('clue_urls', []), ensure_ascii=False),
                json.dumps(item.get('traces', []), ensure_ascii=False),
                item.get('answer_clue'),
                item.get('answer_url'),
                json.dumps(item.get('check_info', {}), ensure_ascii=False),
                json.dumps(item.get('dfsw', {}), ensure_ascii=False),
                current_time,
                uploaded_by,
                'pending'  # 设置默认状态为pending
            )
            prepared_data.append(data_tuple)
        
        return prepared_data
    
    def insert_qa_data(self, data: List[Dict], uploaded_by: str) -> Tuple[int, int, List[str]]:
        """
        插入QA数据到数据库
        
        Args:
            data: 数据列表
            uploaded_by: 上传者
        
        Returns:
            Tuple[int, int, List[str]]: (成功数量, 失败数量, 错误信息列表)
        """
        with self.db_lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            success_count = 0
            failed_count = 0
            error_messages = []
            
            # 开始事务
            cursor.execute('BEGIN TRANSACTION')
            
            try:
                prepared_data = self.prepare_data_for_insert(data, uploaded_by)
                
                for i, data_tuple in enumerate(prepared_data):
                    try:
                        cursor.execute('''
                            INSERT INTO qa_data (
                                question_cn, answer_cn, question_en, answer_en, subject,
                                clue_urls, traces, answer_clue, answer_url, check_info, 
                                dfsw_info, created_at, uploaded_by, annotation_status
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', data_tuple)
                        success_count += 1
                    except Exception as e:
                        failed_count += 1
                        error_msg = f"第{i+1}条数据插入失败: {str(e)}"
                        error_messages.append(error_msg)
                        print(error_msg)
                
                # 提交事务
                cursor.execute('COMMIT')
                conn.close()
                
                return success_count, failed_count, error_messages
                
            except Exception as e:
                # 回滚事务
                cursor.execute('ROLLBACK')
                conn.close()
                error_messages.append(f"事务执行失败: {str(e)}")
                return 0, len(data), error_messages
    
    def upload_from_text(self, data_text: str, uploaded_by: str) -> Dict[str, Any]:
        """
        从文本上传数据
        
        Args:
            data_text: JSON文本数据
            uploaded_by: 上传者
        
        Returns:
            Dict: 上传结果
        """
        try:
            # 解析JSON数据
            success, result = self.parse_json_data(data_text)
            if not success:
                return {
                    'success': False,
                    'message': result,
                    'success_count': 0,
                    'failed_count': 0,
                    'error_details': []
                }
            
            data = result
            
            # 插入数据
            success_count, failed_count, error_messages = self.insert_qa_data(data, uploaded_by)
            
            # 构建响应
            if success_count > 0:
                message = f'成功上传 {success_count} 条数据'
                if failed_count > 0:
                    message += f'，失败 {failed_count} 条'
                
                return {
                    'success': True,
                    'message': message,
                    'success_count': success_count,
                    'failed_count': failed_count,
                    'error_details': error_messages
                }
            else:
                return {
                    'success': False,
                    'message': f'上传失败，错误数量: {failed_count}',
                    'success_count': 0,
                    'failed_count': failed_count,
                    'error_details': error_messages
                }
                
        except Exception as e:
            return {
                'success': False,
                'message': f'处理失败: {str(e)}',
                'success_count': 0,
                'failed_count': 0,
                'error_details': [str(e)]
            }
    
    def upload_from_file(self, file_obj, uploaded_by: str) -> Dict[str, Any]:
        """
        从文件上传数据
        
        Args:
            file_obj: 文件对象
            uploaded_by: 上传者
        
        Returns:
            Dict: 上传结果
        """
        try:
            # 处理文件上传
            success, result = self.process_file_upload(file_obj)
            if not success:
                return {
                    'success': False,
                    'message': result,
                    'success_count': 0,
                    'failed_count': 0,
                    'error_details': []
                }
            
            data_text = result
            
            # 使用文本上传方法处理
            return self.upload_from_text(data_text, uploaded_by)
            
        except Exception as e:
            return {
                'success': False,
                'message': f'文件上传失败: {str(e)}',
                'success_count': 0,
                'failed_count': 0,
                'error_details': [str(e)]
            }
    
    def upload_data(self, file_obj=None, data_text: str = None, uploaded_by: str = "") -> Dict[str, Any]:
        """
        统一的数据上传接口
        
        Args:
            file_obj: 文件对象（可选）
            data_text: 文本数据（可选）
            uploaded_by: 上传者
        
        Returns:
            Dict: 上传结果
        """
        try:
            # 检查参数
            if not uploaded_by:
                return {
                    'success': False,
                    'message': '缺少上传者信息',
                    'success_count': 0,
                    'failed_count': 0,
                    'error_details': []
                }
            
            # 优先处理文件上传
            if file_obj and hasattr(file_obj, 'filename') and file_obj.filename:
                return self.upload_from_file(file_obj, uploaded_by)
            
            # 处理文本数据
            elif data_text and data_text.strip():
                return self.upload_from_text(data_text, uploaded_by)
            
            # 没有提供数据
            else:
                return {
                    'success': False,
                    'message': '请提供JSON数据或上传JSON文件',
                    'success_count': 0,
                    'failed_count': 0,
                    'error_details': []
                }
                
        except Exception as e:
            return {
                'success': False,
                'message': f'上传处理失败: {str(e)}',
                'success_count': 0,
                'failed_count': 0,
                'error_details': [str(e)]
            }
    
    def get_upload_statistics(self) -> Dict[str, Any]:
        """
        获取上传统计信息
        
        Returns:
            Dict: 统计信息
        """
        try:
            with self.db_lock:
                conn = self._get_connection()
                cursor = conn.cursor()
                
                # 总数据量
                cursor.execute(f"SELECT COUNT(*) FROM {self.table_name}")
                total_count = cursor.fetchone()[0] or 0
                
                # 按上传者统计
                cursor.execute(f"""
                    SELECT uploaded_by, COUNT(*) 
                    FROM {self.table_name} 
                    WHERE uploaded_by IS NOT NULL AND uploaded_by != ''
                    GROUP BY uploaded_by
                    ORDER BY COUNT(*) DESC
                """)
                uploader_stats = dict(cursor.fetchall())
                
                # 按学科统计
                cursor.execute(f"""
                    SELECT subject, COUNT(*) 
                    FROM {self.table_name} 
                    WHERE subject IS NOT NULL AND subject != ''
                    GROUP BY subject
                    ORDER BY COUNT(*) DESC
                """)
                subject_stats = dict(cursor.fetchall())
                
                # 按上传时间统计（最近7天）
                cursor.execute(f"""
                    SELECT DATE(created_at) as upload_date, COUNT(*) 
                    FROM {self.table_name} 
                    WHERE created_at >= datetime('now', '-7 days')
                    GROUP BY DATE(created_at)
                    ORDER BY upload_date DESC
                """)
                daily_stats = dict(cursor.fetchall())
                
                conn.close()
                
                return {
                    'total_count': total_count,
                    'uploader_count': len(uploader_stats),
                    'subject_count': len(subject_stats),
                    'uploader_stats': uploader_stats,
                    'subject_stats': subject_stats,
                    'daily_stats': daily_stats
                }
                
        except Exception as e:
            return {
                'error': f'获取统计信息失败: {str(e)}',
                'total_count': 0,
                'uploader_count': 0,
                'subject_count': 0,
                'uploader_stats': {},
                'subject_stats': {},
                'daily_stats': {}
            }
    
    def validate_database_schema(self) -> Tuple[bool, str]:
        """
        验证数据库表结构
        
        Returns:
            Tuple[bool, str]: (是否有效, 错误信息)
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # 检查表是否存在
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{self.table_name}'")
            if not cursor.fetchone():
                conn.close()
                return False, f"表 {self.table_name} 不存在"
            
            # 检查必需字段
            cursor.execute(f"PRAGMA table_info({self.table_name})")
            columns = [column[1] for column in cursor.fetchall()]
            
            required_columns = [
                'id', 'question_cn', 'answer_cn', 'created_at', 
                'uploaded_by', 'annotation_status'
            ]
            
            missing_columns = [col for col in required_columns if col not in columns]
            if missing_columns:
                conn.close()
                return False, f"缺少必需字段: {', '.join(missing_columns)}"
            
            conn.close()
            return True, ""
            
        except Exception as e:
            return False, f"数据库结构验证失败: {str(e)}"
