-- Partie 1 : Calcul du chiffre d'affaires jour par jour pour l'année 2019
SELECT
    t.date AS date,  
    SUM(t.prod_price * t.prod_qty) AS ventes  
FROM
    TRANSACTIONS t
JOIN
    PRODUCT_NOMENCLATURE p ON t.prod_id = p.product_id  
WHERE
    t.date BETWEEN '2019-01-01' AND '2019-12-31'  
GROUP BY
    t.date  
ORDER BY
    t.date; 
