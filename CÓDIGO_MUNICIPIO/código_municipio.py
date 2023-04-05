import pandas as pd

FILES_PATH = "C:/Users/mreis/Documents/MICKAEL-YOSHUA/Medicamentos/Yoshua/CÓDIGO_MUNICIPIO/"

df = pd.read_csv(FILES_PATH + "RELATORIO_DTB_BRASIL_MUNICIPIO.csv", sep=";", encoding="latin-1")
print(df.columns)

df = df.rename(columns={
    "Código Município Completo": "CodigoMunicipio",
    "Nome_Município": "Municipio"
})

df[["CodigoMunicipio", "Municipio"]].to_csv(FILES_PATH + "codigo_municipio.csv", encoding="utf-16", index=False)
print(df.dtypes)