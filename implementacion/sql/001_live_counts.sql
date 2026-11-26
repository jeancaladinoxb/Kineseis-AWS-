-- Ejecutar con la conexión de escritura de la Lambda en airline-db / midterm2.
BEGIN;
DO $$
BEGIN
    IF current_database() <> 'midterm2' THEN
        RAISE EXCEPTION 'Se esperaba la base midterm2';
    END IF;
END $$;
CREATE TABLE IF NOT EXISTS public.live_counts (
    event_id text PRIMARY KEY,
    event_type text NOT NULL CHECK (event_type IN
        ('search_performed', 'reservation_created', 'reservation_cancelled')),
    event_ts timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS live_counts_ts ON public.live_counts (event_ts);
COMMIT;
