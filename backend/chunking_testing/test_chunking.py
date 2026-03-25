"""
Idea : 
    Entire pdf document data split into multiple piplines : 1. Text pipeline, 2. Image pipeline, 3. Tables pipeline and so on

    Text pipeline: 
        1. Extract out all the text from a pdf using pymupdf
        2. Run it through an LLM model to summarise and retrieve back context of entire document
        3. This context will be used to "add-on" to every chunk to provide better semantic quality
        4. Extract out the text from the pdf again but with .get_text('html') to get a structured representation of pdf text
        5. Remove all non-text elements

        Here occurs a branch in methodology.

        Methdology A: Semantic aware chunking that destroys reading order but prioritises semantics
            6. Try to split by sections (perhaps at h1 level)
            7. Pass the whole h1 chunk to a LLM to get back more context
            8. For each section, split into sentences
            9. For each sentence, attach the summarised context
            10. Get embeddings for each chunk, and attach to chunk
            11. Do semantic grouping of each chunk to get chunks similar to each other within a threshold
            12. Concatenate the text for the similar chunks together, and store the full chunk in postgreSQL
            13. Upload chunk with only content text to Qdrant for faster retrieval

        Methodology B: Sliding window chunking with context that provides additional context for each chunk and preserves reading order,
                        but is worse in semantics
            6. Try to split by sections (perhaps at h1 level) (perhaps just at page level)
            7. Pass the whole h1/page chunk to a LLM to get back more context
            8. For each section, split into sentences
            9. Perform sliding window chunking on sentences by section
            10. For each chunk, attach context data
            11. Get embeddings for each chunk
            12. Store chunk in postgreSQL and upload to Qdrant

        Notes:
            Try to use LangChain, use PyMuPDF to chunk, try to see if LangChain has PyMuPDF integration
            Embeddings use Qwen3-VL, check if there is LangChain integration through HuggingFace

    Image pipeline:
        1. Extract out the images from pdf using pymupdf
        2. Filter images out for bad quality images
        3. For each image, pass into a LLM to generate a short summary, and attach the image summary to the chunk under context
        4. Attach pdf summary to the tables chunk under context
        5. Identify the page/section from which the image comes from, and attach the section summary to the image chunk under context
        6. Identify the previous and following text paragraph from the image, and attach together under image content
        7. OCR the image for any text present in the image. If no text, return an empty string
        8. Attach OCR text to image
        9. Embed the image
        10. Store image in postgreSQL and upload to Qdrant

    Tables pipeline:
        1. Extract out tables using pymupdf
        2. Use pymupdf to convert the tables to markdown format for text processing
        3. Attach pdf summary to the tables chunk under context
        4. Identify the page/section from which the image comes from, and attach the section summary to the tables chunk under context
        5. Identify the previous and following text paragraph from the table, and attach together under table content
        6. Embed the table
        7. Store the table in postgreSQL and Qdrant

        """