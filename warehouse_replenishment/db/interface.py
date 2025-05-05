# warehouse_replenishment/db/interface.py
import sys
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List, Type, Union
from pathlib import Path

# Add parent directories to path for imports
parent_dir = str(Path(__file__).parent.parent.parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from warehouse_replenishment.models import Base
from warehouse_replenishment.exceptions import DatabaseError

class DatabaseInterface(ABC):
    """Abstract database interface for different database types."""
    
    @abstractmethod
    def query(self, table_name: str, filters: Dict[str, Any] = None, limit: int = None) -> List[Dict[str, Any]]:
        """Query data from a table."""
        pass
    
    @abstractmethod
    def insert(self, table_name: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Insert data into a table."""
        pass
    
    @abstractmethod
    def update(self, table_name: str, data: Dict[str, Any], filters: Dict[str, Any]) -> int:
        """Update data in a table."""
        pass
    
    @abstractmethod
    def delete(self, table_name: str, filters: Dict[str, Any]) -> int:
        """Delete data from a table."""
        pass
    
    @abstractmethod
    def execute_raw(self, query: str, params: Dict[str, Any] = None) -> Any:
        """Execute raw SQL/query."""
        pass

class SupabaseInterface(DatabaseInterface):
    """Supabase interface implementation."""
    
    def __init__(self, client):
        """Initialize with Supabase client."""
        self.client = client
    
    def query(self, table_name: str, filters: Dict[str, Any] = None, limit: int = None) -> List[Dict[str, Any]]:
        """Query data from a table using Supabase."""
        query = self.client.table(table_name).select('*')
        
        if filters:
            for key, value in filters.items():
                if isinstance(value, list):
                    query = query.in_(key, value)
                else:
                    query = query.eq(key, value)
        
        if limit:
            query = query.limit(limit)
        
        result = query.execute()
        
        if hasattr(result, 'error') and result.error:
            raise DatabaseError(f"Supabase query error: {result.error}")
        
        return result.data if result.data else []
    
    def insert(self, table_name: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Insert data into a table using Supabase."""
        result = self.client.table(table_name).insert(data).execute()
        
        if hasattr(result, 'error') and result.error:
            raise DatabaseError(f"Supabase insert error: {result.error}")
        
        return result.data[0] if result.data else {}
    
    def update(self, table_name: str, data: Dict[str, Any], filters: Dict[str, Any]) -> int:
        """Update data in a table using Supabase."""
        query = self.client.table(table_name).update(data)
        
        for key, value in filters.items():
            query = query.eq(key, value)
        
        result = query.execute()
        
        if hasattr(result, 'error') and result.error:
            raise DatabaseError(f"Supabase update error: {result.error}")
        
        return len(result.data) if result.data else 0
    
    def delete(self, table_name: str, filters: Dict[str, Any]) -> int:
        """Delete data from a table using Supabase."""
        query = self.client.table(table_name).delete()
        
        for key, value in filters.items():
            query = query.eq(key, value)
        
        result = query.execute()
        
        if hasattr(result, 'error') and result.error:
            raise DatabaseError(f"Supabase delete error: {result.error}")
        
        return len(result.data) if result.data else 0
    
    def execute_raw(self, query: str, params: Dict[str, Any] = None) -> Any:
        """Execute raw query using Supabase RPC."""
        result = self.client.rpc(query, params or {}).execute()
        
        if hasattr(result, 'error') and result.error:
            raise DatabaseError(f"Supabase RPC error: {result.error}")
        
        return result.data

class DatabaseAdapter:
    """Adapter that provides unified access to different database backends."""
    
    def __init__(self, connection):
        """Initialize with database connection."""
        self.connection = connection
        self._interface = None
    
    @property
    def interface(self) -> DatabaseInterface:
        """Get appropriate database interface."""
        if self._interface is None:
            if self.connection.db_type == "supabase":
                self._interface = SupabaseInterface(self.connection.get_supabase())
            elif self.connection.db_type == "postgresql":
                # Use SQLAlchemy models directly for PostgreSQL
                self._interface = None
            else:
                raise DatabaseError(f"Unknown database type: {self.connection.db_type}")
        
        return self._interface
    
    def get_by_id(self, model_class: Type[Base], id_value: Any) -> Optional[Base]:
        """Get model instance by ID."""
        if self.connection.db_type == "postgresql":
            with self.connection.session_scope() as session:
                return session.query(model_class).get(id_value)
        else:
            table_name = self._get_table_name(model_class)
            results = self.interface.query(table_name, {'id': id_value}, limit=1)
            return self._dict_to_model(model_class, results[0]) if results else None
    
    def query_all(self, model_class: Type[Base], filters: Dict[str, Any] = None, limit: int = None) -> List[Base]:
        """Query all instances of a model."""
        if self.connection.db_type == "postgresql":
            with self.connection.session_scope() as session:
                query = session.query(model_class)
                if filters:
                    for key, value in filters.items():
                        if isinstance(value, list):
                            query = query.filter(getattr(model_class, key).in_(value))
                        else:
                            query = query.filter(getattr(model_class, key) == value)
                if limit:
                    query = query.limit(limit)
                return query.all()
        else:
            table_name = self._get_table_name(model_class)
            results = self.interface.query(table_name, filters, limit)
            return [self._dict_to_model(model_class, result) for result in results]
    
    def save(self, instance: Base) -> Base:
        """Save model instance."""
        if self.connection.db_type == "postgresql":
            with self.connection.session_scope() as session:
                session.add(instance)
                session.commit()
                session.refresh(instance)
                return instance
        else:
            table_name = self._get_table_name(instance.__class__)
            data = self._model_to_dict(instance)
            
            # Remove id if it's None (for new records)
            if 'id' in data and data['id'] is None:
                del data['id']
            
            if hasattr(instance, 'id') and instance.id:
                self.interface.update(table_name, data, {'id': instance.id})
            else:
                result = self.interface.insert(table_name, data)
                for key, value in result.items():
                    setattr(instance, key, value)
            
            return instance
    
    def delete(self, instance: Base) -> None:
        """Delete model instance."""
        if self.connection.db_type == "postgresql":
            with self.connection.session_scope() as session:
                session.delete(instance)
                session.commit()
        else:
            table_name = self._get_table_name(instance.__class__)
            if hasattr(instance, 'id') and instance.id:
                self.interface.delete(table_name, {'id': instance.id})
    
    def _get_table_name(self, model_class: Type[Base]) -> str:
        """Get table name from model class."""
        if hasattr(model_class, '__tablename__'):
            return model_class.__tablename__
        else:
            return model_class.__name__.lower()
    
    def _model_to_dict(self, instance: Base) -> Dict[str, Any]:
        """Convert model instance to dictionary."""
        result = {}
        for column in instance.__table__.columns:
            value = getattr(instance, column.name)
            if hasattr(value, 'value'):  # Handle enums
                value = value.value
            result[column.name] = value
        return result
    
    def _dict_to_model(self, model_class: Type[Base], data: Dict[str, Any]) -> Base:
        """Convert dictionary to model instance."""
        instance = model_class()
        for column in instance.__table__.columns:
            if column.name in data:
                setattr(instance, column.name, data[column.name])
        return instance