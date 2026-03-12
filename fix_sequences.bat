#!/bin/bash

# Configuration
DB_NAME="yummy2"
DB_USER="postgres"
DB_HOST="localhost"
DB_PORT="5432"

echo "🔧 Fixing PostgreSQL sequences..."

# Connexion à la base de données et exécution
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME << 'EOF'
DO $$
DECLARE
    rec RECORD;
    max_id INTEGER;
BEGIN
    FOR rec IN 
        SELECT 
            t.table_name,
            pg_get_serial_sequence('public.' || t.table_name, 'id') as seq_name
        FROM information_schema.tables t
        WHERE t.table_schema = 'public'
          AND EXISTS (
              SELECT 1 FROM information_schema.columns c
              WHERE c.table_name = t.table_name 
                AND c.column_name = 'id'
                AND c.table_schema = 'public'
          )
    LOOP
        IF rec.seq_name IS NOT NULL THEN
            EXECUTE format('SELECT COALESCE(MAX(id), 0) FROM %I', rec.table_name) 
                INTO max_id;
            
            EXECUTE format('SELECT setval(%L, %s, false)', rec.seq_name, max_id + 1);
            
            RAISE NOTICE 'Fixed: % -> %', rec.table_name, max_id + 1;
        END IF;
    END LOOP;
END $$;
EOF

echo "✅ Done!"