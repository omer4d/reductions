function nop(accum, x) {
    return x;
}

function pack(accum, ...args) {
    return args;
}

function identity(x) {
    return x;
}

function ReducedException(result, include) {
    this.name = "ReducedException";
    this.message = "'reduced' not supported in this context!";
    this.result = result;
    this.include = include;
}

function reduced(x, include) {
    throw new ReducedException(x, include);
}

function handleReducedException(e, callback, data) {
    if(e instanceof ReducedException) {
	callback(e.result, e.include ? function(newData) {
	    callback(e.result, null, newData);
	} : null, data);
    }
    else
	throw e;
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
		    handleReducedException(e, callback, nestedData);
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

// ***************
// * Collections *
// ***************

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
		    handleReducedException(e, callback, data);
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

function range(start, step, end) {
    return {
	reductions: function(r, accum, callback) {
	    var fn = function(x, accum, data) {
		try {
                    if (end !== undefined && (step >= 0 && x >= end || step < 0 && x <= end))
			callback(accum, null, data);
                    else {
			var newAccum = r(accum, x);
			callback(newAccum, function(newData) {
                            fn(x + step, newAccum, newData);
			}, data);
                    }
		}catch(e) {
		    handleReducedException(e, callback, data);
		}
	    };
	    
	    return function(data) {
		fn(start, accum, data);
	    };
	}
    };
}

function streamColl(self) {
    return {
	cache: [],
	reductions: function(r, accum, callback) {
	    var cache = this.cache;
	    
	    var fn1 = function(cache, accum, data) {
		if(self.end())
		    return callback(accum, null, data);
		else {
		    var tmp = self.next();
		    cache.push(tmp);
		    try{
			var newAccum = r(accum, tmp);
			callback(newAccum, function(newData) {
			    fn1(cache, newAccum, newData);
			}, data);
		    }catch(e) {
			handleReducedException(e, callback, data);
		    }
		}
	    };
	    
	    var fn2 = function(cache, idx, accum, data) {
		if(idx >= cache.length)
		    return fn1(cache, accum, data);
		else {
		    try {
			var newAccum = r(accum, cache[idx]);
			callback(newAccum, function(newData) {
			    fn2(cache, idx + 1, newAccum, newData);
			}, data);
		    }catch(e) {
			handleReducedException(e, callback, data);
		    }
		}
	    };
	    
	    return function(data) {
		fn2(cache, 0, accum, data);
	    };
	}
    };
};

// **************
// * Operations *
// **************

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

function take(n0, coll) {
    return {
	reductions: function(r, accum, callback) {
	    var n = n0;
	    
	    if(n < 1)
		return function(data) {
		    callback(accum, null, data);
		};
	    
	    return coll.reductions(function(accum, x) {
		if(n === 0) {
		    reduced(accum);
		}else if(n === 1) {
		    --n;
		    reduced(r(accum, x), true);
		}else {
		    --n;
		    return r(accum, x);
		}
	    }, accum, callback);
	}
    };
}

function drop(n0, coll) {
    return {
	reductions: function(r, accum, callback) {
	    var n = n0;
	    
	    return coll.reductions(function(accum, x) {
		if(n > 0) {
		    --n;
		    return accum;
		}else {
		    return r(accum, x);
		}
	    }, accum, callback);
	}
    };
}

function concat(a, b) {
    return {
	reductions: function(r, accum, callback) {
	    return a.reductions(r, accum, function(e, next, data) {
		console.log(e);
		
		if(next) {
		    callback(e, next, data);
		}
		else {
		    b.reductions(r, e, callback)(data);
		}
	    });
	}
    };
}

function count(coll, callback) {
    return reduce(function(accum, x) {
	return accum + 1;
    }, 0, coll, callback);
}

function nth(n, coll, callback) {
    return reduce(function(accum, x) {
	if(n < 0) {
	    reduced(accum);
	}else if(n === 0) {
	    --n;
	    return x;
	}else {
	    --n;
	    return accum;
	}
    }, undefined, coll, callback);
}
