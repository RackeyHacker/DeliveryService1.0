CREATE TABLE IF NOT EXISTS outbox (
 sequence BIGSERIAL UNIQUE NOT NULL,
 event_id VARCHAR(36) PRIMARY KEY, routing_key VARCHAR(80) NOT NULL, payload TEXT NOT NULL,
 created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS suppliers (id BIGINT PRIMARY KEY, name VARCHAR(200) NOT NULL);
CREATE TABLE IF NOT EXISTS products (
 id BIGINT PRIMARY KEY, supplier_id BIGINT NOT NULL REFERENCES suppliers(id),
 name VARCHAR(200) NOT NULL, price DECIMAL(12,2) NOT NULL CHECK(price>0),
 available_quantity INT NOT NULL CHECK(available_quantity>=0)
);
INSERT INTO suppliers(id,name) VALUES (1,'Учебный поставщик') ON CONFLICT DO NOTHING;
INSERT INTO products(id,supplier_id,name,price,available_quantity) VALUES
 (1,1,'Ноутбук',65000.00,20),(2,1,'Мышь',1500.00,100) ON CONFLICT DO NOTHING;
