SELECT 
    c.data,
    c.chuva_mm,
    c.temp_max,
    m.preco_saca_reais as preco_milho
FROM `monitor-passofundo.clima_dados.historico_diario` AS c
LEFT JOIN `monitor-passofundo.clima_dados.precos_milho_cepea` AS m
    ON CAST(c.data AS DATE) = CAST(m.data AS DATE)
ORDER BY c.data DESC