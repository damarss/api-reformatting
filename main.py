from typing import Union
from fastapi import FastAPI, File, UploadFile
import pandas as pd
import pdfplumber
import io
from fastapi.responses import FileResponse

app = FastAPI()

table_settings1 = {
    "vertical_strategy": "lines",  # Try different strategies ('lines', 'text')
    "horizontal_strategy": "lines",
    "intersection_tolerance": 5  # Adjust this tolerance for table cell detection
}

@app.get("/")
def read_root():
    return {"status": "success", "message": "API is active"}

@app.post("/merge")
async def merge(pdf_file: UploadFile, excel_file: UploadFile):
    """
    Merge file laporan pdf and referensi excel
    """
    try:
        # file laporan
        content_pdf = await pdf_file.read()
        pdf_file_like = io.BytesIO(content_pdf)

        with pdfplumber.open(pdf_file_like) as pdf:
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

            
        # file referensi
        content_excel = await excel_file.read()
        read = pd.read_excel(content_excel)
        read.columns = ['1','2','3','4','5','6','7','8','9','10','11','12','13']
        ref_table = read[['2', '3', '5', '6']]
        ref_table.columns = ['Kode Barang', 'Kode Grup', 'Satuan', 'Deskripsi']
        ref_table = ref_table.dropna(thresh=len(ref_table.columns)-1)
        ref_table = ref_table[ref_table['Kode Barang'].str[:2] == "00"]
        
        final_table = pd.merge(table_df, ref_table, on=['Kode Barang', 'Deskripsi'], how='left')
        final_table.to_excel("merge.xlsx", index=False)

        return FileResponse("merge.xlsx")
        return pdf_file.filename
    except Exception as e:
        return {"status": "error", "message": str(e)}