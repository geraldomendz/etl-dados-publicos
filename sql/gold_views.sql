
-- Pergunta respondida --> Quanto foi gasto por tipo de transferência?
CREATE OR REPLACE VIEW vw_total_valor_por_tipo_transferencia AS
SELECT
    d.descricao AS tipo_transferencia,
    COUNT(f.doc_key) AS qtd_documentos,
    SUM(f.valor) AS valor_total
FROM fato_documento_despesa f
LEFT JOIN dim_tipo_transferencia d
    ON f.tipo_transferencia_id = d.id
GROUP BY d.descricao;



-- Pergunta respondida --> Quantos documentos existem por mês?
CREATE OR REPLACE VIEW vw_documentos_por_mes AS
SELECT
    ano,
    mes,
    COUNT(doc_key) AS qtd_documentos
FROM fato_documento_despesa
GROUP BY ano, mes
ORDER BY ano, mes;



-- Pergunta resondida --> De qual arquivo bruto veio cada registro?
CREATE OR REPLACE VIEW vw_auditoria_documentos AS
SELECT
    doc_key,
    raw_json_ref
FROM fato_documento_despesa;
