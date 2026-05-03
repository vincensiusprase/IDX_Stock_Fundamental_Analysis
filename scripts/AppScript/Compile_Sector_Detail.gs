function compileDetailsSheets() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const targetSheetName = "COMPILE_DETAILS";
  
  // 1. Siapkan Sheet Target (COMPILE_DETAILS)
  let targetSheet = ss.getSheetByName(targetSheetName);
  if (!targetSheet) {
    targetSheet = ss.insertSheet(targetSheetName);
  } else {
    targetSheet.clear(); 
  }

  // 2. Daftar konfigurasi sheet
  const sheetsToStack = [
    { name: "IDXTECHNODetails", startRow: 1 }, 
    { name: "IDXNONCYCDetails", startRow: 2 },
    { name: "IDXCYCLICDetails", startRow: 2 },
    { name: "IDXINDUSTDetails", startRow: 2 },
    { name: "IDXFINANCEDetails", startRow: 2 },
    { name: "IDXBASICDetails", startRow: 2 },
    { name: "IDXENERGYDetails", startRow: 2 },
    { name: "IDXHEALTHDetails", startRow: 2 },
    { name: "IDXINFRADetails", startRow: 2 },
    { name: "IDXTRANSDetails", startRow: 2 }
  ];

  let compiledData = [];

  // 3. Looping ke setiap sheet
  sheetsToStack.forEach(config => {
    let sheet = ss.getSheetByName(config.name);
    
    if (sheet) {
      // --- PENDETEKSI REAL LAST ROW BERDASARKAN KOLOM A ---
      // Ambil seluruh data di kolom A untuk dicek
      let colA = sheet.getRange("A:A").getValues();
      let realLastRow = 0;
      
      // Cek dari baris paling bawah ke atas, cari sel pertama yang tidak kosong
      for (let r = colA.length - 1; r >= 0; r--) {
        if (colA[r][0].toString().trim() !== "") {
          realLastRow = r + 1; // +1 karena index array dimulai dari 0
          break;
        }
      }
      // ----------------------------------------------------

      // Pastikan realLastRow mencapai batas startRow (ada datanya)
      if (realLastRow >= config.startRow) {
        let numRows = realLastRow - config.startRow + 1;
        
        // Tarik data HANYA sampai baris terakhir yang terisi (A sampai FH = 164 kolom)
        let data = sheet.getRange(config.startRow, 1, numRows, 164).getValues();
        
        // Filter ekstra: Buang jika di tengah-tengah ada baris yang kolom A-nya kosong
        let filteredData = data.filter(row => row[0].toString().trim() !== "");
        
        // Gabungkan ke array utama
        if (filteredData.length > 0) {
          compiledData = compiledData.concat(filteredData);
          Logger.log(`[SUKSES] Menarik ${filteredData.length} baris dari ${config.name} (Terbaca sampai baris ke-${realLastRow})`);
        }
      } else {
        Logger.log(`[SKIP] Sheet ${config.name} kosong (tidak ada data di bawah baris start).`);
      }
    } else {
      Logger.log(`[SKIP] Sheet ${config.name} tidak ditemukan. Diabaikan.`);
    }
  });

  // 4. Tulis semua data ke sheet target (COMPILE_DETAILS) sekaligus
  if (compiledData.length > 0) {
    // --- TAMBAHAN: KUNCI FORMAT KOLOM E SEBAGAI TEXT ---
    targetSheet.getRange("E:E").setNumberFormat("@");

    // Tulis data ke sheet
    targetSheet.getRange(1, 1, compiledData.length, compiledData[0].length).setValues(compiledData);
    Logger.log(`Berhasil menggabungkan total ${compiledData.length} baris ke sheet ${targetSheetName}.`);
  } else {
    Logger.log(`Peringatan: Tidak ada data valid yang ditemukan untuk digabung.`);
  }
}
