import pypyodbc
import pandas as pd

def query(query):
    conn = pypyodbc.connect(Driver='{SQL Server Native Client 11.0}',
                        Server='ben.acpafl.org',
                        Database= 'pacs_training',
                        Trusted_Connection='NO',
                        UID="tbatsql",
                        PWD="Pr0metheus623")
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df
    
def querywrite(query):
    conn = pypyodbc.connect(Driver='{SQL Server Native Client 11.0}',
                        Server='ben.acpafl.org',
                        Database= 'pacs_training',
                        Trusted_Connection='NO',
                        UID="tbatsql",
                        PWD="Pr0metheus623")
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    conn.close()

df = query('''
            select *
            from dbo.situs
            where situs_city is not null
            and situs_city != ''
            and primary_situs = 'Y'
            AND prop_id in ( 
            select prop_id
            from dbo.situs
            where primary_situs = 'N'
            AND situs_city is null
            UNION
            select prop_id
            from dbo.situs
            where primary_situs = 'N'
            AND situs_city = '')
            ''')
rows = len(df.index)
#rows = 1
i = 0
while i < rows:
    dfx = df.iloc[[i]]
    print("PROCESSING " + dfx["prop_id"].to_string(index=False))
    prop_id = dfx["prop_id"].to_string(index=False)
    
    df2query = ('''select prop_id, primary_situs, situs_id, situs_city, situs_state, situs_zip
    from dbo.situs
    where prop_id = {0}
    order by primary_situs desc''').format(prop_id)
    #print(df2query)
    df2 = query(df2query)
    rows2 = len(df2.index)
    #print("There is this many rows " + str(rows2))
    dfx2 = df2.iloc[[0]]
    city = dfx2["situs_city"].to_string(index=False)
    state = dfx2["situs_state"].to_string(index=False)
    zip = dfx2["situs_zip"].to_string(index=False)
    prop_id = dfx2["prop_id"].to_string(index=False)
    ii = 1
    while ii < rows2:
        dfx2 = df2.iloc[[ii]]
        situs_id = dfx2["situs_id"].to_string(index=False)
        query22 = ('''
        UPDATE pacs_training.dbo.situs            
        SET situs_city = '{0}', situs_zip = '{1}', situs_state = '{2}'
        WHERE prop_id = {3}
        AND situs_id = {4}
        ''').format(city,zip,state,prop_id,situs_id)
        print(query22)
        querywrite(query22)
        ii += 1
    i += 1