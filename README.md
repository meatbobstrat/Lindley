@"
# Lindley Ingestor

OCR pipeline for historical archive documents.

This project ingests scanned images (JPG/PNG/TIFF) or image-only PDFs, 
converts them into **searchable PDF/A** files, and organizes them 
into output folders. It uses:

- **watcher**: monitors the input folder for new files, enqueues jobs
- **worker**: processes jobs with [OCRmyPDF](https://ocrmypdf.readthedocs.io/)
  and [Tesseract OCR](https://github.com/tesseract-ocr/tesseract)
- **redis**: lightweight queue for job distribution
- **docker-compose**: ties it all together

---

## Quickstart

1. **Start Docker Desktop**  
   Ensure Docker Desktop is running before continuing.

2. **Clone the repo**  
   ```powershell
   git clone https://github.com/<your-username>/Lindley-Ingestor.git
   cd Lindley-Ingestor
