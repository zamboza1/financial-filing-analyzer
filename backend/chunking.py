"""Document chunking with metadata."""

from typing import List
from backend.entities import Filing, DocumentChunk
from backend.text_clean import TextExtractor


class DocumentChunker:
    """
    Splits documents into chunks for vector indexing.
    
    Uses a simple strategy: split by paragraphs, then combine into
    fixed-size chunks with overlap. This keeps chunks semantically
    coherent while maintaining consistent sizes for indexing.
    
    Representation Invariants:
    - chunk_size > 0
    - chunk_overlap >= 0 and < chunk_size
    """
    
    def __init__(
        self,
        chunk_size: int = 1000,
        chunk_overlap: int = 200,
        min_chunk_size: int = 100
    ) -> None:
        """
        Initialize chunker with size parameters.
        
        Preconditions:
        - chunk_size > 0
        - chunk_overlap >= 0 and < chunk_size
        - min_chunk_size > 0 and <= chunk_size
        
        Postconditions:
        - Chunker is configured with valid parameters
        
        Args:
            chunk_size: Target size for chunks (in characters)
            chunk_overlap: Overlap between chunks (in characters)
            min_chunk_size: Minimum chunk size (smaller chunks are discarded)
        """
        if chunk_size <= 0:
            raise ValueError(f"chunk_size must be positive, got {chunk_size}")
        if chunk_overlap < 0:
            raise ValueError(f"chunk_overlap must be non-negative, got {chunk_overlap}")
        if chunk_overlap >= chunk_size:
            raise ValueError(f"chunk_overlap ({chunk_overlap}) must be < chunk_size ({chunk_size})")
        if min_chunk_size <= 0 or min_chunk_size > chunk_size:
            raise ValueError(f"min_chunk_size must be in (0, chunk_size], got {min_chunk_size}")
        
        self._chunk_size = chunk_size
        self._chunk_overlap = chunk_overlap
        self._min_chunk_size = min_chunk_size
        self._extractor = TextExtractor()
    
    def chunk_filing(self, filing: Filing) -> List[DocumentChunk]:
        """
        Chunk a filing into DocumentChunk objects.
        
        Preconditions:
        - filing.raw_text_path exists and is readable
        
        Postconditions:
        - Returns list of DocumentChunk objects
        - Each chunk has unique chunk_id
        - Chunks are ordered by position in document
        - All chunks meet minimum size requirement
        
        Args:
            filing: Filing to chunk
            
        Returns:
            List of DocumentChunk objects
            
        Raises:
            ValueError: If filing path is invalid or text extraction fails
        """
        if filing.raw_text_path is None:
            raise ValueError(f"Filing {filing.accession} has no raw_text_path")
        
        # Extract text from filing
        text = self._extractor.extract_from_filing(filing.raw_text_path)
        
        # Split into chunks
        chunks = self._split_text(text, filing)
        
        return chunks
    
    def _split_text(self, text: str, filing: Filing) -> List[DocumentChunk]:
        """
        Split text into chunks with metadata.
        
        Strategy:
        1. Split by paragraphs (double newlines)
        2. Combine paragraphs into chunks of target size
        3. Add overlap between chunks
        4. Create DocumentChunk objects with metadata
        
        Args:
            text: Text to split
            filing: Source filing for metadata
            
        Returns:
            List of DocumentChunk objects
        """
        # Split into paragraphs (preserve paragraph structure)
        paragraphs = [p.strip() for p in text.split("\n\n") if p.strip()]
        
        if not paragraphs:
            raise ValueError("No paragraphs found in text")
        
        chunks = []
        current_chunk_parts = []
        current_size = 0
        chunk_index = 0
        
        for para in paragraphs:
            para_size = len(para)
            
            # Handle case where single paragraph exceeds chunk size
            if para_size > self._chunk_size:
                # Finalize current chunk if it exists
                if current_chunk_parts:
                    chunk_text = "\n\n".join(current_chunk_parts)
                    if len(chunk_text) >= self._min_chunk_size:
                        chunk = self._create_chunk(chunk_text, filing, chunk_index)
                        chunks.append(chunk)
                        chunk_index += 1
                    current_chunk_parts = []
                    current_size = 0
                
                # Split oversized paragraph into multiple chunks
                para_chunks = self._split_oversized_paragraph(para, filing, chunk_index)
                chunks.extend(para_chunks)
                chunk_index += len(para_chunks)
                continue
            
            # If adding this paragraph would exceed chunk size, finalize current chunk
            if current_size + para_size > self._chunk_size and current_chunk_parts:
                # Create chunk from accumulated parts
                chunk_text = "\n\n".join(current_chunk_parts)
                if len(chunk_text) >= self._min_chunk_size:
                    chunk = self._create_chunk(chunk_text, filing, chunk_index)
                    chunks.append(chunk)
                    chunk_index += 1
                
                # Start new chunk with overlap
                if self._chunk_overlap > 0 and current_chunk_parts:
                    # Keep last part(s) for overlap
                    overlap_text = "\n\n".join(current_chunk_parts[-2:])  # Last 2 paragraphs
                    if len(overlap_text) <= self._chunk_overlap:
                        current_chunk_parts = [overlap_text]
                        current_size = len(overlap_text)
                    else:
                        # Take suffix of last paragraph for overlap
                        last_para = current_chunk_parts[-1]
                        overlap_suffix = last_para[-self._chunk_overlap:]
                        current_chunk_parts = [overlap_suffix]
                        current_size = len(overlap_suffix)
                else:
                    current_chunk_parts = []
                    current_size = 0
            
            # Add paragraph to current chunk
            current_chunk_parts.append(para)
            current_size += para_size + 2  # +2 for "\n\n"
        
        # Add final chunk if it meets minimum size
        if current_chunk_parts:
            chunk_text = "\n\n".join(current_chunk_parts)
            if len(chunk_text) >= self._min_chunk_size:
                chunk = self._create_chunk(chunk_text, filing, chunk_index)
                chunks.append(chunk)
        
        if not chunks:
            raise ValueError("No chunks created (all chunks below minimum size)")
        
        return chunks
    
    def _split_oversized_paragraph(
        self,
        para: str,
        filing: Filing,
        start_index: int
    ) -> List[DocumentChunk]:
        """
        Split a paragraph that exceeds chunk_size into multiple chunks.
        
        Args:
            para: Paragraph text to split
            filing: Source filing
            start_index: Starting chunk index
            
        Returns:
            List of DocumentChunk objects
        """
        chunks = []
        chunk_index = start_index
        offset = 0
        
        while offset < len(para):
            # Calculate chunk end (with overlap consideration)
            chunk_end = min(offset + self._chunk_size, len(para))
            chunk_text = para[offset:chunk_end]
            
            # Only create chunk if it meets minimum size
            if len(chunk_text) >= self._min_chunk_size:
                chunk = self._create_chunk(chunk_text, filing, chunk_index)
                chunks.append(chunk)
                chunk_index += 1
            
            # Move offset forward (with overlap)
            if chunk_end >= len(para):
                break
            offset = chunk_end - self._chunk_overlap
            if offset <= 0:
                offset = chunk_end  # Prevent infinite loop
        
        return chunks
    
    def _create_chunk(
        self,
        text: str,
        filing: Filing,
        chunk_index: int
    ) -> DocumentChunk:
        """
        Create a DocumentChunk with proper ID and metadata.
        
        Args:
            text: Chunk text content
            filing: Source filing
            chunk_index: Index of chunk in document
            
        Returns:
            DocumentChunk object
        """
        # Create unique chunk ID: {ticker}_{accession}_chunk_{index}
        accession_clean = filing.accession.replace("-", "_")
        chunk_id = f"{filing.company.ticker}_{accession_clean}_chunk_{chunk_index}"
        
        return DocumentChunk(
            chunk_id=chunk_id,
            text=text,
            source_filing=filing,
            chunk_index=chunk_index
        )
