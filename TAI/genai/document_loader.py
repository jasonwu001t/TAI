import os
import pandas as pd
from PyPDF2 import PdfReader
from docx import Document
from langchain.embeddings import OpenAIEmbeddings
from langchain.vectorstores import FAISS
from langchain.document_loaders import CSVLoader, PDFLoader, DocxLoader, ExcelLoader, ParquetLoader

class KnowledgeBaseAgent:
    def __init__(self):
        self.documents = []
        self.vector_store = None

    def load_documents(self, folder_path):
        """
        Load documents from a given folder and create embeddings.

        Args:
        folder_path (str): The path to the folder containing the documents.

        Returns:
        None
        """
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            if filename.endswith('.csv'):
                loader = CSVLoader(file_path)
            elif filename.endswith('.pdf'):
                loader = PDFLoader(file_path)
            elif filename.endswith('.docx'):
                loader = DocxLoader(file_path)
            elif filename.endswith('.xlsx'):
                loader = ExcelLoader(file_path)
            elif filename.endswith('.parquet'):
                loader = ParquetLoader(file_path)
            else:
                continue

            documents = loader.load()
            self.documents.extend(documents)

        self._create_embeddings()

    def _create_embeddings(self):
        """
        Create embeddings for the loaded documents.

        Returns:
        None
        """
        embeddings = OpenAIEmbeddings()
        self.vector_store = FAISS.from_documents(self.documents, embeddings)

    def query_knowledge_base(self, query):
        """
        Query the knowledge base to find the most relevant documents.

        Args:
        query (str): The user's query.

        Returns:
        str: The generated response.
        """
        if self.vector_store is None:
            return "Knowledge base is empty. Please load documents first."

        docs = self.vector_store.similarity_search(query, k=3)
        response = "\n".join([doc.page_content for doc in docs])
        return response
