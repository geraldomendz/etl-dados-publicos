-- Cria tabela dim_tipo_transferencia
CREATE TABLE IF NOT EXISTS dim_tipo_transferencia (
    id INT PRIMARY KEY,
    descricao VARCHAR(255) NOT NULL
);


-- Cria tabela fato_documento_despesa
CREATE TABLE IF NOT EXISTS fato_documento_despesa (
    doc_key VARCHAR(100) PRIMARY KEY,
    ano INT NULL,
    mes INT NULL,
    valor DECIMAL(18,2) NULL,
    tipo_transferencia_id INT NULL,
    raw_json_ref VARCHAR(255) NULL,

    CONSTRAINT fk_fato_tipo_transferencia
        FOREIGN KEY (tipo_transferencia_id)
        REFERENCES dim_tipo_transferencia(id)
);
