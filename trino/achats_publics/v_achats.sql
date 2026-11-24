-- Virtual counterpart of gold.achats_publics.achats: same rows, computed at query time
-- from silver and the reference tables. Run once in Trino, by a user who can read silver.
CREATE OR REPLACE VIEW gold.achats_publics.v_achats AS
SELECT
    a.annee_notification,
    a.mois_notification,
    d.code_region,
    COALESCE(d.libelle_region, 'Non déterminée') AS libelle_region,
    d.code_carte_region,
    b.code_departement,
    COALESCE(d.libelle_departement, 'Non déterminé') AS libelle_departement,
    d.code_carte_departement,
    b.type_acheteur,
    CASE a.titulaire_categorie_entreprise
        WHEN 'PME' THEN 'PME'
        WHEN 'ETI' THEN 'ETI'
        WHEN 'GE' THEN 'Grande entreprise'
        ELSE 'Non renseigné'
    END AS categorie_entreprise,
    CASE
        WHEN a.titulaire_code_departement IS NULL OR b.code_departement IS NULL THEN NULL
        ELSE a.titulaire_code_departement = b.code_departement
    END AS achat_local,
    m.procedure,
    m.accord_cadre,
    m.clause_sociale,
    m.clause_environnementale,
    a.part_marche,
    a.montant_attribue AS montant
FROM silver.achats_publics.attributions a
JOIN silver.achats_publics.marches m
    ON m.id_marche = a.id_marche AND m.acheteur_siret = a.acheteur_siret
LEFT JOIN silver.referentiels.acheteurs b ON b.acheteur_siret = a.acheteur_siret
LEFT JOIN silver.referentiels.geo_departements d ON d.code_departement = b.code_departement
