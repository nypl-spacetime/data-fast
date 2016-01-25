var fs = require('fs');
var path = require('path');
var H = require('highland');
var N3 = require('n3');

function ndjson(obj) {
  var pit = {
    type: 'pit',
    obj: {
      uri: obj.id,
      type: 'hg:Place'
    }
  };

  var names = [];
  if (obj.names.length) {
    names = obj.names.map(function(name) {
      return name.replace(/^"/, '').replace(/"$/, '');
    });

    pit.obj.name = names[0];
    // TODO: add other names as new PITs! Or allow more names per PIT!!!!
  }

  var items = [pit];

  var relations = obj.sameAs.concat(obj.relatedMatch).map(function(uri) {
    var numberMatch = /.*\/(\d*)$/.exec(uri);

    if (numberMatch) {
      if (!uri.startsWith('http://viaf.org/')) {
        // URI is a GeoNames ID!
        var geoNamesId = numberMatch[1];
        uri = 'http://sws.geonames.org/' + geoNamesId + '/';
      }
    }

    return {
      type: 'relation',
      obj: {
        from: obj.id,
        to: uri,
        type: 'hg:sameAs'
      }
    }
  });

  items.push(relations);

  return items;
}

// This function assumes NT file to be ordered by object!
var subject;
var triples = {};
function newTriple(triple)  {
  var obj;

  newSubject = triple.subject;

  if (newSubject !== subject) {
    if (triples[subject]) {
      // obj is finished! set return value!
      obj = Object.assign({}, triples[subject]);
    }

    delete triples[subject];
    triples[newSubject] = {
      id: triple.subject,
      names: [],
      sameAs: [],
      relatedMatch: []
    };
  }

  subject = newSubject;

  // <http://id.worldcat.org/fast/1318589> <http://schema.org/name> "Florida--Fort Lauderdale--Fort Lauderdale Beach" .
  if (triple.predicate === 'http://schema.org/name') {
    triples[subject].names.push(triple.object);
  }

  // <http://id.loc.gov/authorities/names/nr2001042090> <http://www.w3.org/2000/01/rdf-schema#label> "J\u014Dzai-han (Japan)" .
  if (triple.predicate === 'http://www.w3.org/2000/01/rdf-schema#label') {
    triples[subject].names.push(triple.object);
  }

  // <http://id.worldcat.org/fast/1280227> <http://schema.org/sameAs> <http://viaf.org/viaf/137377076> .
  if (triple.predicate === 'http://schema.org/sameAs') {
    triples[subject].sameAs.push(triple.object);
  }

  //<http://id.worldcat.org/fast/1215726> <http://www.w3.org/2004/02/skos/core#relatedMatch> <http://id.loc.gov/authorities/names/5485605> .
  // (Data is wrong! baseUrl is incorrect, this is in fact a GeoNames URI!)
  if (triple.predicate === 'http://www.w3.org/2004/02/skos/core#relatedMatch') {
    triples[subject].relatedMatch.push(triple.object);
  }

  return obj;
}

var writeLine = function(writer, obj, callback) {
  writer.writeObject(obj, function(err) {
    callback(err);
  });
};

// TODO: download NT file!
// function download(config, dir, writer, callback) {
// }

function convert(config, dir, writer, callback) {
  var streamParser = new N3.StreamParser();
  var inputStream = fs.createReadStream(path.join(__dirname, 'FASTGeographic.nt'));
  inputStream.pipe(streamParser);

  var ndjsonItems = H(streamParser)
    .filter(function(triple) {
      return triple.subject.startsWith('http://id.worldcat.org/fast/');
    })
    .consume(function (err, triple, push, next) {
      if (err) {
        push(err);
        next();
      } else if (triple === H.nil) {
        push(null, triple);
      } else {
        var obj = newTriple(triple)
        if (obj) {
          push(null, obj);
        }
        next();
      }
    })
    .map(ndjson)
    .flatten()
    .map(H.curry(writeLine, writer))
    .nfcall([])
    .series()
    .errors(function(err){
      console.error(err);
    })
    .done(function() {
      callback();
    });
}

// ==================================== API ====================================

module.exports.steps = [
  // download,
  convert
];
