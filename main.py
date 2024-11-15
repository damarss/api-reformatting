from typing import Union
from fastapi import FastAPI, File, UploadFile, HTTPException, Request
import pandas as pd
import pdfplumber
import io
from fastapi.responses import FileResponse, JSONResponse

app = FastAPI()

table_settings1 = {
    "vertical_strategy": "lines",  # Try different strategies ('lines', 'text')
    "horizontal_strategy": "lines",
    "intersection_tolerance": 5  # Adjust this tolerance for table cell detection
}

@app.get("/")
def read_root():
    return {"status": "success", "message": "API is active"}

@app.post("/laporan")
async def reformat_laporan(file: UploadFile):
    """
    Reformatting laporan dari file pdf
    """
    if not file.filename.endswith(".pdf"):
        raise HTTPException(status_code=400, detail="File harus dalam format pdf")

    # file laporan
    content = await file.read()
    file_like = io.BytesIO(content)

    with pdfplumber.open(file_like) as pdf:
        all_tables = []  # To store tables from each page
        for page in pdf.pages:
            table = page.extract_tables(table_settings=table_settings1)  # Extract table from each page
            all_tables.append(table)

    flattened_tables1 = []

    for table in all_tables:
        for row in table:
            flattened_tables1.append(row)

    cols = ["Kode Barang", "Deskripsi", "Jumlah Awal Tahun", "Jumlah Awal Tahun (Rp)", "Mutasi Masuk", "Mutasi Keluar", "Jumlah Mutasi", "Jumlah Akhir Tahun", "Jumlah Akhir Tahun (Rp)"]

    if all_tables[0]:
        table_df = pd.concat([pd.DataFrame(table[1:], columns=cols) for table in flattened_tables1], ignore_index=True)
        table_df = table_df[table_df['Kode Barang'].str[:2] == "00"]
        table_df.to_excel("laporan.xlsx", index=False)

        return FileResponse("laporan.xlsx")

    raise HTTPException(status_code=404, detail="Tidak ada tabel dalam laporan")


@app.post("/referensi")
async def reformat_referensi(file: UploadFile):
    """
    Reformatting referensi dari bentuk excel
    """
    if not file.filename.endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="File harus dalam format xlsx")

    # file referensi
    content = await file.read()
    read = pd.read_excel(content)
    read.columns = ['1','2','3','4','5','6','7','8','9','10','11','12','13']
    ref_table = read[['2', '3', '5', '6']]
    ref_table.columns = ['Kode Barang', 'Kode Grup', 'Satuan', 'Deskripsi']
    ref_table = ref_table.dropna(thresh=len(ref_table.columns)-1)
    ref_table = ref_table[ref_table['Kode Barang'].str[:2] == "00"]

    ref_table.to_excel("referensi.xlsx", index=False)

    return FileResponse("referensi.xlsx")


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={"message": "Internal Server Error"},
    )
