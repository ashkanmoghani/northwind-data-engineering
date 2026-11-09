def geography_key(client, pg, values):
    fields = ("country", "region", "city", "postalcode", "address")
    params = {k: (v or "") for k, v in zip(fields, values)}
    with pg, pg.cursor() as cur:
        cur.execute("SELECT pg_advisory_xact_lock(hashtext('classic_dimgeography'))")
        predicate = " AND ".join(f"ifNull({k}, '') = %({k})s" for k in fields)
        rows = client.query(
            "SELECT geographykey FROM dimgeography WHERE " + predicate + " LIMIT 1",
            parameters=params,
        ).result_rows
        if rows:
            return rows[0][0]
        key = client.command("SELECT ifNull(max(geographykey),0)+1 FROM dimgeography")
        client.insert(
            "dimgeography", [[key, *values]], column_names=["geographykey", *fields]
        )
        return key
