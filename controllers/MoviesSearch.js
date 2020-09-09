const fs = require("fs");
const elasticsearch = require("elasticsearch");
const esClient = new elasticsearch.Client({
  host: "127.0.0.1:9200",
  log: "error"
});

//API for Movies Search
exports.search = (req, res) => {
  const { title, date_published, genre, duration, country, director, language, sort_duration, sort_duration_asc, sort_date_asc } = req.query;

  let obj = {};
  let searchIndex = 0;
  if (!title && !date_published && !genre && !duration && !country && !director && !language) {
    obj["match_all"] = {};
  }

  if (title || date_published || duration || director) {
    obj["bool"] = {
      must: []
    };
  }

  if (title) {
    obj.bool.must[searchIndex++] = {
      match: {
        title: {
          query: req.query.title,
          minimum_should_match: 3,
          fuzziness: 2
        }
      }
    };
  }

  if (date_published) {
    obj.bool.must[searchIndex++] = {
      match: {
        date_published: {
          query: req.query.date_published
        }
      }
    };
  }

  if (duration) {
    obj.bool.must[searchIndex++] = {
      match: {
        duration: {
          query: req.query.duration
        }
      }
    };
  }

  if (director) {
    obj.bool.must[searchIndex++] = {
      match: {
        director: {
          query: req.query.director,
          minimum_should_match: 3,
          fuzziness: 2
        }
      }
    };
  }

  if (language) {
    obj.bool.must[searchIndex++] = {
      match: {
        language: {
          query: req.query.language
        }
      }
    };
  }

  console.log(obj);
  const search = function search(index, body) {
    return esClient.search({ index: index, body: body });
  };

  // only for testing purposes
  // all calls should be initiated through the module
  const test = function test() {
    let body = {
      size: 20,
      from: 0,
      query: obj
    };

    // console.log(`retrieving documents whose title matches '${body.query.match.title.query}' (displaying ${body.size} items at a time)...`);
    search("library", body)
      .then(results => {
        console.log(results);
        console.log(`found ${results.hits.total} items in ${results.took}ms`);
        if (results.hits.total > 0) console.log(`returned article titles:`);
        results.hits.hits.forEach((hit, index) => console.log(`\t${body.from + ++index} - ${hit._source.title} (score: ${hit._score})`));

        let output = results.hits.hits;
        if (sort_duration && sort_duration_asc) {
          output = output.sort((a, b) => {
            return a._source.date_published - b._source.date_published;
          });
        } else if (sort_duration && !sort_duration_asc) {
          output = output.sort((a, b) => {
            return b._source.date_published - a._source.date_published;
          });
        }

        if (sort_date_asc) {
          output = output.sort((a, b) => {
            return a._source.date_published - b._source.date_published;
          });
        } else {
          output = output.sort((a, b) => {
            return a._source.date_published - b._source.date_published;
          });
        }
        res.json(output);
      })
      .catch(console.error);
  };

  test();
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
