-- Purchases by region and share of SMEs, on the materialized datamart...
SELECT libelle_region,
       ROUND(SUM(montant) / 1e6) AS montant_meur,
       ROUND(100 * SUM(CASE WHEN categorie_entreprise = 'PME' THEN montant END) / SUM(montant), 1) AS part_pme
FROM gold.achats_publics.achats
WHERE annee_notification = 2025
GROUP BY libelle_region
ORDER BY montant_meur DESC;

-- ...and on the view, computed from silver at query time.
SELECT libelle_region,
       ROUND(SUM(montant) / 1e6) AS montant_meur,
       ROUND(100 * SUM(CASE WHEN categorie_entreprise = 'PME' THEN montant END) / SUM(montant), 1) AS part_pme
FROM gold.achats_publics.v_achats
WHERE annee_notification = 2025
GROUP BY libelle_region
ORDER BY montant_meur DESC;
