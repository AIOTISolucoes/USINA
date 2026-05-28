-- Migração: Favoritar seleções no Data Studio
-- Rodar manualmente no Postgres antes de deploy da api2.py

-- Coluna de favorito + dono do favorito
ALTER TABLE app.user_selection
  ADD COLUMN IF NOT EXISTS is_favorite BOOLEAN NOT NULL DEFAULT false;

ALTER TABLE app.user_selection
  ADD COLUMN IF NOT EXISTS favorited_by TEXT;

ALTER TABLE app.user_selection
  ADD COLUMN IF NOT EXISTS favorited_at TIMESTAMPTZ;

-- Index para listar favoritos rápido
CREATE INDEX IF NOT EXISTS ix_user_selection_fav
  ON app.user_selection (customer_id, is_favorite, favorited_by);
