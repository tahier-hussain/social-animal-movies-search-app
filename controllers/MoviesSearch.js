const fs = require("fs");
const elasticsearch = require("elasticsearch");
const esClient = new elasticsearch.Client({
  host: "127.0.0.1:9200",
  log: "error"
});
const paginate = require("jw-paginate");

//API for Movies Search
exports.search = (req, res) => {
  const { page, title, date_published, duration, director, genre, country, language, sort_duration, sort_duration_asc, sort_date, sort_date_asc } = req.query;

  console.log(req.query);
  let query = {};
  let sort = [];
  let must_index = 0;
  let filter_index = 0;

  if (!title && !date_published && !genre && !duration && !country && !director && !language) {
    query["bool"] = {
      must: [
        {
          match: {
            year: {
              query: "2019"
            }
          }
        },
        {
          match: {
            language: {
              query: "english"
            }
          }
        }
      ]
    };
  }

  if (title || date_published || duration || director || genre || country || language) {
    query["bool"] = {
      must: [],
      filter: []
    };
  }

  //Search for Movies by Title, Date of published, Duration and Director
  if (title) {
    let title_token = title.split(" ");
    let minimum_should_match;
    if (title_token.length >= 3) {
      minimum_should_match = 3;
    } else {
      minimum_should_match = title_token.length;
    }
    query.bool.must[must_index++] = {
      match: {
        title: {
          query: title,
          operator: "and",
          fuzziness: "auto"
        }
      }
    };
  }

  if (date_published) {
    query.bool.must[must_index++] = {
      match: {
        date_published: {
          query: date_published
        }
      }
    };
  }

  if (duration) {
    query.bool.must[must_index++] = {
      match: {
        duration: {
          query: duration
        }
      }
    };
  }

  if (director) {
    query.bool.must[must_index++] = {
      match: {
        director: {
          query: director,
          operator: "and",
          fuzziness: "auto"
        }
      }
    };
  }

  //Filter movies by Country, Genre and Language
  if (country) {
    query.bool.filter[filter_index++] = {
      match: {
        country: {
          query: country
        }
      }
    };
  }

  if (genre) {
    query.bool.filter[filter_index++] = {
      match: {
        genre: {
          query: genre
        }
      }
    };
  }

  if (language) {
    query.bool.filter[filter_index++] = {
      match: {
        language: {
          query: language
        }
      }
    };
  }

  const search = function search(index, body) {
    return esClient.search({ index: index, body: body });
  };

  const test = function test() {
    let from_index = 0;
    if (page) {
      if (page > 0) {
        from_index = page - 1;
      }
    }
    let body = {
      size: 1000,
      from: from_index,
      query,
      sort
    };
    console.log(body);

    search("library", body)
      .then(results => {
        let output = results.hits.hits;

        //Sort by Reviews from users (for initial list of movies)
        if (!title && !date_published && !genre && !duration && !country && !director && !language && !sort_duration && !sort_date) {
          output.sort((a, b) => {
            let movieA_reviews_from_users = 0;
            if (a._source.reviews_from_users.length > 0) {
              movieA_reviews_from_users = parseFloat(a._source.reviews_from_users.length);
            }
            let movieA_reviews_from_critics = 0;
            if (a._source.reviews_from_critics.length > 0) {
              movieA_reviews_from_critics = parseFloat(b._source.reviews_from_critics);
            }
            let movieA_worlwide_gross_income = 0;
            if (a._source.worlwide_gross_income.length > 0) {
              movieA_worlwide_gross_income = parseFloat(a._source.worlwide_gross_income.slice(2, a._source.worlwide_gross_income.length));
            }

            let movieB_reviews_from_users = 0;
            if (b._source.reviews_from_users.length > 0) {
              movieB_reviews_from_users = parseFloat(b._source.reviews_from_users.length);
            }
            let movieB_reviews_from_critics = 0;
            if (b._source.reviews_from_critics.length > 0) {
              movieB_reviews_from_critics = parseFloat(b._source.reviews_from_critics);
            }
            let movieB_worlwide_gross_income = 0;
            if (b._source.worlwide_gross_income.length > 0) {
              movieB_worlwide_gross_income = parseFloat(b._source.worlwide_gross_income.slice(2, b._source.worlwide_gross_income.length));
            }

            console.log(movieB_reviews_from_users + movieB_reviews_from_critics + movieB_worlwide_gross_income - (movieA_reviews_from_users + movieA_reviews_from_critics + movieA_worlwide_gross_income));
            return movieB_reviews_from_users + movieB_reviews_from_critics + movieB_worlwide_gross_income - (movieA_reviews_from_users + movieA_reviews_from_critics + movieA_worlwide_gross_income);
          });
        }

        //Sort by Date
        if (sort_date === "true" && sort_date_asc === "true") {
          output.sort((a, b) => {
            let dateA = new Date(a._source.date_published),
              dateB = new Date(b._source.date_published);
            return dateA - dateB;
          });
        } else if (sort_date === "true" && sort_date_asc === "false") {
          output.sort((a, b) => {
            let dateA = new Date(a._source.date_published),
              dateB = new Date(b._source.date_published);
            return dateB - dateA;
          });
        }

        //Sort by Duration
        if (sort_duration === "true" && sort_duration_asc === "true") {
          output.sort((a, b) => {
            return a._source.duration - b._source.duration;
          });
        } else if (sort_duration === "true" && sort_duration_asc === "false") {
          output.sort((a, b) => {
            return b._source.duration - a._source.duration;
          });
        }

        const page = parseInt(req.query.page) || 1;
        const pager = paginate(output.length, page, 20);
        const pageOfMovies = output.slice(pager.startIndex, pager.endIndex + 1);
        res.json({ status: 200, pager, pageOfMovies });
      })
      .catch(err => {
        console.log(err);
        res.json({ status: 400, data: "Something went wrong" });
      });
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
