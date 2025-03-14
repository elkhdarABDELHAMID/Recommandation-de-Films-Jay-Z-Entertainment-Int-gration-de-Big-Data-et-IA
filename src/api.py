from flask import Flask, request, jsonify
from elasticsearch import Elasticsearch
import logging
import os
from functools import wraps
import time

# Configuration
ES_HOST = os.environ.get("ES_HOST", "http://elasticsearch:9200")
ES_INDEX = os.environ.get("ES_INDEX", "movies")
API_PORT = int(os.environ.get("API_PORT", 5000))
API_HOST = os.environ.get("API_HOST", "0.0.0.0")
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")

# Configure logging
numeric_level = getattr(logging, LOG_LEVEL.upper(), None)
if not isinstance(numeric_level, int):
    numeric_level = logging.INFO

logging.basicConfig(
    level=numeric_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Elasticsearch connection
es = None

def init_elasticsearch(max_retries=5, delay=5):
    """Initialize Elasticsearch connection with retry logic"""
    global es
    retry_count = 0
    while retry_count < max_retries:
        try:
            logger.info(f"Attempting to connect to Elasticsearch at {ES_HOST} (Attempt {retry_count + 1}/{max_retries})")
            es = Elasticsearch([ES_HOST], timeout=30)
            if es.ping():
                logger.info("Successfully connected to Elasticsearch")
                return True
            else:
                logger.warning("Elasticsearch ping failed")
        except Exception as e:
            logger.error(f"Failed to connect to Elasticsearch: {e}")
        
        retry_count += 1
        if retry_count < max_retries:
            time.sleep(delay)  # Wait before retrying
    logger.error("Max retries reached. Failed to connect to Elasticsearch.")
    return False

# Decorator to ensure Elasticsearch is available
def require_elasticsearch(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if es is None or not es.ping():
            logger.error("Elasticsearch connection is not available")
            return jsonify({"error": "Service temporarily unavailable"}), 503
        return f(*args, **kwargs)
    return decorated_function

# Exception handling
@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Endpoint not found"}), 404

@app.errorhandler(500)
def internal_server_error(error):
    logger.error(f"Internal server error: {error}")
    return jsonify({"error": "Internal server error"}), 500

# Routes
@app.route("/recommend", methods=["POST"])
@require_elasticsearch
def recommend():
    """Get movie recommendations based on a title"""
    data = request.get_json()
    if not data:
        return jsonify({"error": "Invalid JSON"}), 400
    
    movie_title = data.get("title")
    if not movie_title:
        logger.warning("Recommendation request missing title")
        return jsonify({"error": "Title is required"}), 400
    
    logger.info(f"Received recommendation request for title: {movie_title}")
    
    try:
        # Search for movie by title
        res = es.search(index=ES_INDEX, query={
            "match_phrase": {"title": movie_title}
        }, size=5)
        
        hits = res["hits"]["hits"]
        if not hits:
            logger.warning(f"Movie not found: {movie_title}")
            return jsonify({"error": "Movie not found"}), 404
        
        # Check if multiple movies were found
        if len(hits) > 1:
            logger.info(f"Multiple movies found for title: {movie_title}")
            return jsonify({
                "message": "Multiple movies found, please select one",
                "movies": [{"movieId": hit["_source"]["movieId"], "title": hit["_source"]["title"]} for hit in hits]
            }), 200
        
        # Get movie information
        movie = hits[0]["_source"]
        movie_id = movie["movieId"]
        
        # Process genres
        genres = movie.get("genres", [])
        
        # Debug log
        logger.info(f"Movie found: {movie_title}, ID: {movie_id}, Genres: {genres}")
        
        # Build recommendation query
        if not genres:
            # Alternative method if no genres available
            logger.warning(f"No genres available for movie: {movie_title}. Using title-based recommendation.")
            
            # Extract keywords from title
            title_words = [word for word in movie_title.split() if len(word) > 3]
            
            # Build a query to find movies with similar words in the title
            should_clauses = [{"match": {"title": word}} for word in title_words]
            
            rec_query = {
                "bool": {
                    "should": should_clauses,
                    "must_not": {"term": {"movieId": movie_id}},
                    "minimum_should_match": 1
                }
            }
        else:
            # Genre-based method
            rec_query = {
                "bool": {
                    "must": [
                        {"terms": {"genres": genres}},
                        {"bool": {"must_not": {"term": {"movieId": movie_id}}}}
                    ]
                }
            }
        
        # Search for recommendations
        logger.debug(f"Recommendation query: {rec_query}")
        recs = es.search(index=ES_INDEX, query=rec_query, size=5)
        
        # Extract results
        recommended_movies = [hit["_source"] for hit in recs["hits"]["hits"]]
        
        logger.info(f"Returning {len(recommended_movies)} recommendations for movieId: {movie_id}")
        return jsonify({
            "movie": movie,
            "recommendations": recommended_movies
        }), 200
    
    except Exception as e:
        logger.error(f"Error in recommendation: {str(e)}", exc_info=True)
        return jsonify({"error": f"Error processing recommendation: {str(e)}"}), 500

@app.route("/movie/<movie_id>", methods=["GET"])
@require_elasticsearch
def get_movie(movie_id):
    """Get details for a specific movie by ID"""
    try:
        # Ensure movie_id is properly formatted
        movie_id = str(movie_id)
        
        # Search for movie by ID
        res = es.search(index=ES_INDEX, query={"term": {"movieId": movie_id}})
        
        hits = res["hits"]["hits"]
        if not hits:
            logger.warning(f"Movie not found: movieId {movie_id}")
            return jsonify({"error": "Movie not found"}), 404
            
        logger.info(f"Returning details for movieId: {movie_id}")
        return jsonify(hits[0]["_source"]), 200
    except Exception as e:
        logger.error(f"Error retrieving movie {movie_id}: {str(e)}", exc_info=True)
        return jsonify({"error": f"Error retrieving movie: {str(e)}"}), 500

@app.route("/search", methods=["GET"])
@require_elasticsearch
def search_movies():
    """Search for movies by title"""
    query = request.args.get("q")
    if not query:
        logger.warning("Search request missing query")
        return jsonify({"error": "Query parameter 'q' is required"}), 400
    
    try:
        # Pagination parameters
        page = int(request.args.get("page", 1))
        size = int(request.args.get("size", 10))
        
        # Validate pagination parameters
        if page < 1:
            page = 1
        if size < 1 or size > 100:
            size = 10
            
        # Calculate from for pagination
        from_param = (page - 1) * size
        
        # Search for movies
        res = es.search(
            index=ES_INDEX, 
            query={
                "multi_match": {
                    "query": query, 
                    "fields": ["title^3", "genres"],
                    "fuzziness": "AUTO"
                }
            },
            from_=from_param,
            size=size
        )
        
        # Extract results
        hits = res["hits"]["hits"]
        total = res["hits"]["total"]["value"] if "value" in res["hits"]["total"] else res["hits"]["total"]
        
        movies = [hit["_source"] for hit in hits]
        
        logger.info(f"Returning {len(movies)} search results for query: {query}")
        return jsonify({
            "movies": movies,
            "page": page,
            "size": size,
            "total": total
        }), 200
    except Exception as e:
        logger.error(f"Error in search: {str(e)}", exc_info=True)
        return jsonify({"error": f"Error during search: {str(e)}"}), 500

@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint"""
    es_health = "OK" if es and es.ping() else "NOT CONNECTED"
    
    return jsonify({
        "status": "OK",
        "elasticsearch": es_health,
        "version": "1.0.0"
    }), 200 if es_health == "OK" else 503

@app.route("/", methods=["GET"])
def index():
    """API documentation endpoint"""
    return jsonify({
        "status": "API running",
        "version": "1.0.0",
        "endpoints": {
            "/recommend": "POST - Get recommendations for a movie (requires title in JSON body)",
            "/movie/<id>": "GET - Get details for a specific movie",
            "/search": "GET - Search for movies (requires q parameter, optional page and size)",
            "/health": "GET - Check API and Elasticsearch health"
        }
    }), 200

if __name__ == "__main__":
    if init_elasticsearch():
        logger.info(f"Starting API server on {API_HOST}:{API_PORT}")
        app.run(host=API_HOST, port=API_PORT)
    else:
        logger.error("Failed to initialize Elasticsearch. Exiting.")
        exit(1)