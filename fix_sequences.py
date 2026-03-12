from django.core.management.base import BaseCommand
from django.db import connection

class Command(BaseCommand):
    help = 'Fix PostgreSQL sequences for all tables'

    def handle(self, *args, **options):
        with connection.cursor() as cursor:
            # Récupérer toutes les tables avec séquences
            cursor.execute("""
                SELECT 
                    table_name,
                    column_name,
                    pg_get_serial_sequence(table_schema || '.' || table_name, column_name) as sequence_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND column_name = 'id'
                  AND pg_get_serial_sequence(table_schema || '.' || table_name, column_name) IS NOT NULL
                ORDER BY table_name;
            """)
            
            tables = cursor.fetchall()
            
            for table_name, column_name, sequence_name in tables:
                if not sequence_name:
                    continue
                    
                # Obtenir le max ID
                cursor.execute(f"SELECT COALESCE(MAX(id), 0) FROM {table_name}")
                max_id = cursor.fetchone()[0]
                
                # Réinitialiser la séquence
                new_value = max_id + 1
                cursor.execute(f"SELECT setval('{sequence_name}', {new_value})")
                
                self.stdout.write(
                    self.style.SUCCESS(
                        f'✓ {table_name}: sequence reset to {new_value}'
                    )
                )
        
        self.stdout.write(self.style.SUCCESS('\n✓ All sequences fixed!'))