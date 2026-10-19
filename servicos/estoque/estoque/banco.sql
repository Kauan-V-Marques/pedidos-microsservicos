CREATE TABLE IF NOT EXISTS produtos (
    sku         text PRIMARY KEY,
    nome        text           NOT NULL,
    preco       numeric(10, 2) NOT NULL CHECK (preco >= 0),
    disponivel  integer        NOT NULL CHECK (disponivel >= 0)
);

CREATE TABLE IF NOT EXISTS reservas (
    pedido_id   uuid    NOT NULL,
    sku         text    NOT NULL REFERENCES produtos (sku),
    quantidade  integer NOT NULL CHECK (quantidade > 0),
    status      text    NOT NULL CHECK (status IN ('reservada', 'confirmada', 'liberada')),
    atualizada_em timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (pedido_id, sku)
);

CREATE TABLE IF NOT EXISTS outbox (
    id           uuid PRIMARY KEY,
    tipo         text        NOT NULL,
    pedido_id    uuid        NOT NULL,
    mensagem     jsonb       NOT NULL,
    criado_em    timestamptz NOT NULL DEFAULT clock_timestamp(),
    publicado_em timestamptz
);
CREATE INDEX IF NOT EXISTS outbox_pendentes ON outbox (criado_em) WHERE publicado_em IS NULL;

CREATE TABLE IF NOT EXISTS mensagens_processadas (
    id            uuid PRIMARY KEY,
    processada_em timestamptz NOT NULL DEFAULT now()
);

-- Catálogo inicial (só na primeira vez)
INSERT INTO produtos (sku, nome, preco, disponivel) VALUES
    ('SEN-QTR8A',  'Sensor de refletância QTR-8A', 189.90, 20),
    ('SEN-HCSR04', 'Sensor ultrassônico HC-SR04',   14.90, 50),
    ('MOT-N20-1K', 'Motor N20 1000 rpm',            39.90, 40),
    ('DRV-TB6612', 'Ponte H TB6612FNG',             24.90, 30),
    ('MCU-ESP32',  'ESP32 DevKit V1',               54.90, 25),
    ('BAT-2S-850', 'Bateria LiPo 2S 850 mAh',       89.90, 15),
    ('KIT-SUMO',   'Kit robô de sumô 3 kg',       4890.00,  3)
ON CONFLICT (sku) DO NOTHING;
