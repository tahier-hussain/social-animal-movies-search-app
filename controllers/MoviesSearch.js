const fs = require("fs");
let csvToJson = require("convert-csv-to-json");
const elasticsearch = require("elasticsearch");
const esClient = new elasticsearch.Client({
  host: "127.0.0.1:9200",
  log: "error"
});

//API for Movies Search
exports.search = (req, res) => {
  const { title, release_date, genre, duration, country, director, language } = req.query;
};

//API for indexing the data
exports.indexData = (req, res) => {
  const bulkIndex = function bulkIndex(index, type, movies) {
    let bulkBody = [];

    movies.forEach(movie => {
      bulkBody.push({
        index: {
          _index: index,
          _type: type,
          _id: movie.imdb_title_id
        }
      });

      bulkBody.push(movie);
    });

    esClient
      .bulk({ body: bulkBody })
      .then(response => {
        console.log(response);
        let errorCount = 0;
        response.items.forEach(movie => {
          if (movie.index && movie.index.error) {
            console.log(++errorCount, movie.index.error);
          }
        });
        console.log(`Successfully indexed ${movies.length - errorCount} out of ${movies.length} movies`);
      })
      .catch(console.err);
  };

  const index = function index() {
    const moviesData = fs.readFileSync("data.json");
    const movies = JSON.parse(moviesData);
    console.log(`${movies.length} items parsed from data file`);
    bulkIndex("library", "article", movies);
  };

  index();
};
