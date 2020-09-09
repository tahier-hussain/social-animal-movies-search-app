const fs = require("fs");
const elasticsearch = require("elasticsearch");
const esClient = new elasticsearch.Client({
  host: "127.0.0.1:9200",
  log: "error"
});

//API for Movies Search
exports.search = (req, res) => {
  const { title, date_published, duration, director, genre, country, language, sort_duration, sort_duration_asc, sort_date, sort_date_asc } = req.query;

  let obj = {};
  let must_index = 0;
  let filter_index = 0;
  if (!title && !date_published && !genre && !duration && !country && !director && !language) {
    obj["match_all"] = {};
  }

  if (title || date_published || duration || director || genre || country || language) {
    obj["bool"] = {
      must: [],
      filter: []
    };
  }

  //Search for Movies by Title, Date of published, Duration and Director
  if (title) {
    obj.bool.must[must_index++] = {
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
    obj.bool.must[must_index++] = {
      match: {
        date_published: {
          query: req.query.date_published
        }
      }
    };
  }

  if (duration) {
    obj.bool.must[must_index++] = {
      match: {
        duration: {
          query: req.query.duration
        }
      }
    };
  }

  if (director) {
    obj.bool.must[must_index++] = {
      match: {
        director: {
          query: req.query.director,
          minimum_should_match: 3,
          fuzziness: 2
        }
      }
    };
  }

  //Filter movies by Country, Genre and Language
  if (country) {
    obj.bool.filter[filter_index++] = {
      match: {
        country: {
          query: req.query.language
        }
      }
    };
  }

  if (genre) {
    obj.bool.filter[filter_index++] = {
      match: {
        genre: {
          query: req.query.genre
        }
      }
    };
  }

  if (language) {
    obj.bool.filter[filter_index++] = {
      match: {
        language: {
          query: req.query.language
        }
      }
    };
  }

  //Sort
  let sort = [];

  //Sort By Date
  if (sort_date && sort_date_asc) {
    sort[0] = {
      date_published: {
        order: "asc"
      }
    };
  } else if (sort_date && !sort_duration) {
    sort[0] = {
      date_published: {
        order: "desc"
      }
    };
  }

  //Sort by Duration
  if (sort_duration && sort_duration_asc) {
    sort[0] = {
      duration: {
        order: "asc"
      }
    };
  } else if (sort_duration && !sort_duration_asc) {
    sort[0] = {
      duration: {
        order: "desc"
      }
    };
  }

  const search = function search(index, body) {
    return esClient.search({ index: index, body: body });
  };

  // only for testing purposes
  // all calls must be initiated through the module
  const test = function test() {
    let body = {
      size: 20,
      from: (req.query.page - 1 || 0) * 20,
      query: obj,
      sort: sort
    };

    search("library", body)
      .then(results => {
        res.json(results.hits.hits);
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
