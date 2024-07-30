# Project Setup

## Setup

To set up the project, follow these steps:

1. Install the required dependencies by running the following command:

   ```sh
   pip install -r requirements.txt
   ```

2. Install the necessary browsers for Playwright by running:

   ```sh
   playwright install
   ```

3. Create a `.env` file in the root directory of the project with the following content. Replace the placeholder values with your actual keys:

   ```env
   GEMINI_API_KEYS=your_gemini_api_key1,your_gemini_api_key2
   OXYLABS_USERNAME=your_oxylabs_username
   OXYLABS_PASSWORD=your_oxylabs_password
   GOOGLE_API_KEY=your_google_api_key
   SEARCH_ENGINE_ID=your_search_engine_id
   ```

   **Note:** The `GEMINI_API_KEYS` can contain multiple keys separated by commas, not just two. This helps distribute requests and avoid hitting rate limits. I would recommend that you use at least 3-4 keys.
 
