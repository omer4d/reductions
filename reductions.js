function nop(accum, x) {
    return x;
}

function pack(accum, ...args) {
    return args;
}

function identity(x) {
    return x;
}

function ReducedException(result) {
    this.name = "ReducedException";
    this.message = "'reduced' not supported in this context!";
    this.result = result;
}

function reduced(x) {
    throw new ReducedException(x);
}

function reductions(r, accum, callback, colls) {
    var sentinel = Object.create(null);
    var job = function(e, next, data) {
        var rowRest = data.rowRest;
        var nextRow = data.nextRow;
        var args = data.args.concat(e);
        var accum = data.accum;
        var nestedData = data.nestedData;

        if (next === null)
            callback(accum, null, nestedData);
        else {
            if (rowRest.length === 0) {
                var newRow = nextRow.concat(next);
		try {
                    var newAccum = r.apply(null, [accum].concat(args));
                    callback(newAccum, function(newNestedData) {
			newRow[0]({
                            rowRest: newRow.slice(1),
                            nextRow: [],
                            args: [],
                            accum: newAccum,
                            nestedData: newNestedData
			});
                    }, nestedData);
		}catch(e) {
		    if(e instanceof ReducedException)
			callback(e.result, null, nestedData);
		    else
			throw e;
		}
            } else {
                rowRest[0]({
                    rowRest: rowRest.slice(1),
                    nextRow: nextRow.concat(next),
                    args: args,
                    accum: accum,
                    nestedData: nestedData
                });
            }
        }
    };

    var rs = colls.map(function(coll) {
        return coll.reductions(pack, sentinel, job);
    });

    return function(data) {
        rs[0]({
            rowRest: rs.slice(1),
            nextRow: [],
            args: [],
            accum: accum,
            nestedData: data
        });
    };
}


function parallel(...colls) {
    return {
	reductions: function(r, accum, callback) {
	    return reductions(r, accum, callback, colls);
	}
    };
}

// **************
// * Operations *
// **************

function collFromArray(arr) {
    return {
	reductions: function(r, accum, callback) {
	    var fn = function(idx, accum, data) {
		try{
                    if (idx >= arr.length)
			callback(accum, null, data);
                    else {
			var newAccum = r(accum, arr[idx]);
			callback(newAccum, function(newData) {
                            fn(idx + 1, newAccum, newData);
			}, data);
                    }
		}catch(e) {
		    if(e instanceof ReducedException) {
			callback(e.result, null, data);
		    }
		    else
			throw e;
		}
	    };
	    
	    return function(data) {
		fn(0, accum, data);
	    };
	}
    };
}

function coll(x) {
    if(x instanceof Array)
	return collFromArray(x);
    else
	return x.collection();
}

function reduce(r, accum, coll, callback) {
    var res, done, callback;
    
    coll.reductions(r, accum, function(accum, next) {
	if(next)
	    next();
	else {
	    res = accum;
	    done = true;
	    if(callback)
		callback(res);
	}
    })();
    
    return done || callback ? res : {
	done: function() {
	    return done;
	},
	result: function(cb) {
	    if(cb) {
		callback = cb;
		if(done) {
		    cb(res);
		}
	    }
	    return res;
	}
    };
}

function collToArray(coll, callback) {
    var res = [];
    
    return reduce(function(accum, ...args) {
	accum.push(args.length < 2 ? args[0] : args);
	return accum;
    }, res, coll, callback);
}

function map(f, coll) {
    return {
	reductions: function(r, accum, callback) {
	    return coll.reductions(function(accum, ...args) {
		return r(accum, f.apply(null, args));
	    }, accum, callback);
	}
    };
}

function filter(p, coll) {
    return {
	reductions: function(r, accum, callback) {
	    return coll.reductions(function(accum, x) {
		return p(x) ? r(accum, x) : accum;
	    }, accum, callback);
	}
    };
}

function take(n, coll) {
    return {
	reductions: function(r, accum, callback) {
	    return coll.reductions(function(accum, x) {
		if(n > 0) {
		    --n;
		    return r(accum, x);
		}
		else {
		    reduced(accum);
		}
	    }, accum, callback);
	}
    };
}
