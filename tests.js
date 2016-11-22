function div(x, y) {
    return x / y;
}

function push(arr, x) {
    arr = arr.slice();
    arr.push(x);
    return arr;
}

function double(x) {
    return x * 2;
}

function even(x) {
    return (x % 2) === 0;
}

function asyncCollFromArray(arr) {
    return {
	reductions: function(r, accum, callback) {
	    var fn = function(idx, accum, data) {
		try{
                    if (idx >= arr.length)
			callback(accum, null, data);
                    else {
			var newAccum = r(accum, arr[idx]);
			setTimeout(function() {
			    callback(newAccum, function(newData) {
				fn(idx + 1, newAccum, newData);
			    }, data);
			}, 100);
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

function testReductions(assert, coll, f, initial, expectations, stopAfter) {
    var counter = 0;
    var tmp = expectations;
    expectations = expectations.slice();
    expectations.push(tmp.length > 0 ? tmp[tmp.length - 1] : initial);
    
    var done = assert.async(expectations.length);
    
    coll.reductions(function(accum, ...args) {
	return stopAfter && counter === stopAfter ? reduced(accum) : f.apply(null, [accum].concat(args));
    }, initial, function(accum, next, data) {
	++counter;
	console.log(counter);
	assert.deepEqual(accum, expectations[counter - 1], "accum " + counter);
	assert.deepEqual(data, counter, "data " + counter);
	if(counter === expectations.length) {
	    assert.notOk(next, "next " + counter);
	}
	else {
	    assert.ok(next, "next " + counter);
	    next(data + 1);
	}
	done();
    })(1);
}

// **************************
// * Array Collection Tests *
// **************************

QUnit.module("Array Collection");

QUnit.test("creation", function(assert) {
    assert.ok(coll([]).reductions, "from []");
    assert.ok(coll(new Array()).reductions, "from new Array()");
});

QUnit.test("reductions (empty)", function(assert) {
    testReductions(assert, coll([]), div, "foo", []);
});


QUnit.test("reductions", function(assert) {
    testReductions(assert, coll([2, 3, 4]), div, 1, [1/2, 1/2/3, 1/2/3/4]);
});

QUnit.test("reductions (empty, immediate stop)", function(assert) {
    testReductions(assert, coll([]), div, "foo", [], 1);
});

QUnit.test("reductions (immediate stop)", function(assert) {
    testReductions(assert, coll([2, 3, 4]), div, 1, [1/2], 1);
});

QUnit.test("reductions (stop)", function(assert) {
    testReductions(assert, coll([2, 3, 4, 5, 6, 7, 8]), div, 1, [1/2, 1/2/3, 1/2/3/4], 3);
});

// ********************************
// * Async Array Collection Tests *
// ********************************

QUnit.module("Async Array Collection");

QUnit.test("reductions (empty)", function(assert) {
    testReductions(assert, asyncCollFromArray([]), div, "foo", []);
});

QUnit.test("reductions", function(assert) {
    testReductions(assert, asyncCollFromArray([2, 3, 4]), div, 1, [1/2, 1/2/3, 1/2/3/4]);
});

// *****************************
// * Parallel Collection Tests *
// *****************************

QUnit.module("Parallel Collection");

QUnit.test("creation", function(assert) {
    assert.ok(parallel(coll([])).reductions, "from single");
    assert.ok(parallel(coll([]), coll([])).reductions, "from multiple");
});

QUnit.test("reductions (single empty)", function(assert) {
    testReductions(assert, parallel(coll([])), div, "foo", []);
});

QUnit.test("reductions (single)", function(assert) {
    testReductions(assert, parallel(coll([2, 3, 4])), div, 1, [1/2, 1/2/3, 1/2/3/4]);
});

QUnit.test("reductions (two, both empty)", function(assert) {
    testReductions(assert, parallel(coll([]), coll([])), div, "foo", []);
});

QUnit.test("reductions (two, first empty)", function(assert) {
    testReductions(assert, parallel(coll([]), coll([1, 2, 3])), div, "foo", []);
});

QUnit.test("reductions (two, second empty)", function(assert) {
    testReductions(assert, parallel(coll([1, 2, 3]), coll([])), div, "foo", []);
});

QUnit.test("reductions (two, same length, ignore accum)", function(assert) {
    testReductions(assert, parallel(coll([1, 2, 3]), coll([2, 3, 4])), function(accum, x, y) {
	return x * y;
    }, [], [2, 6, 12]);
});

QUnit.test("reductions (two, same length)", function(assert) {
    testReductions(assert, parallel(coll([1, 2, 3]), coll([2, 3, 4])), function(accum, x, y) {
	return push(accum, x * y);
    }, [], [[2], [2, 6], [2, 6, 12]]);
});

QUnit.test("reductions (two, first shorter)", function(assert) {
    testReductions(assert, parallel(coll([1, 2]), coll([2, 3, 4])), function(accum, x, y) {
	return push(accum, x * y);
    }, [], [[2], [2, 6]]);
});

QUnit.test("reductions (two, second shorter)", function(assert) {
    testReductions(assert, parallel(coll([1, 2, 3]), coll([2, 3])), function(accum, x, y) {
	return push(accum, x * y);
    }, [], [[2], [2, 6]]);
});

QUnit.test("reductions (nested parallel)", function(assert) {
    testReductions(assert, parallel(parallel(coll([1, 2, 3]), coll(["a", "b", "c"])),
				    parallel(coll([true, false, null]), coll(["x", "y", "z"]))),
		   function(accum, a, b, c, d) {
		       return [a, b, c, d];
		   }, [], [[1, "a", true, "x"],
			   [2, "b", false, "y"],
			   [3, "c", null, "z"]]);
});

QUnit.test("reductions (nested parallel, one shorter)", function(assert) {
    testReductions(assert, parallel(parallel(coll([1, 2, 3]), coll(["a", "b", "c"])),
				    parallel(coll([true, false]), coll(["x", "y", "z"]))),
		   function(accum, a, b, c, d) {
		       return [a, b, c, d];
		   }, [], [[1, "a", true, "x"],
			   [2, "b", false, "y"]]);
});

QUnit.test("reductions (two, one async)", function(assert) {
    testReductions(assert, parallel(asyncCollFromArray([1, 2, 3]), coll([2, 3, 4])), function(accum, x, y) {
	return push(accum, x * y);
    }, [], [[2], [2, 6], [2, 6, 12]]);
});

QUnit.test("reductions (two, both async)", function(assert) {
    testReductions(assert, parallel(asyncCollFromArray([1, 2, 3]), asyncCollFromArray([2, 3, 4])), function(accum, x, y) {
	return push(accum, x * y);
    }, [], [[2], [2, 6], [2, 6, 12]]);
});

QUnit.test("reductions (single, stop early)", function(assert) {
    testReductions(assert, parallel(coll([2, 3, 4, 5, 6, 7, 8])), div, 1, [1/2, 1/2/3, 1/2/3/4], 3);
});

QUnit.test("reductions (two async, stop early)", function(assert) {
    testReductions(assert, parallel(asyncCollFromArray([1, 2, 3, 4, 5, 6]), asyncCollFromArray([2, 3, 4, 5, 6, 7])), function(accum, x, y) {
	return push(accum, x * y);
    }, [], [[2], [2, 6], [2, 6, 12]], 3);
});

// ******************************
// * Collection Operation Tests *
// ******************************


QUnit.module("Operations");

QUnit.test("reduce", function(assert) {
    assert.equal(reduce(div, "foo", coll([])), "foo", "empty collection");
    assert.equal(reduce(div, 1, coll([2, 3, 4])), 1/2/3/4, "sync collection");

    var done = assert.async(4);

    reduce(div, 1, asyncCollFromArray([2, 3, 4])).result(function(x) {
	assert.equal(x, 1/2/3/4, "async collection");
	done();
    });
    
    assert.equal(reduce(div, 1, asyncCollFromArray([2, 3, 4]), function(x) {
	assert.equal(x, 1/2/3/4, "async collection (callback)");
	done();
    }), undefined, "async collection return (callback)");

    assert.equal(reduce(div, "foo", asyncCollFromArray([]), function(x) {
	assert.equal(x, "foo", "async collection (empty, callback)");
	done();
    }), "foo", "async collection return (empty, callback)");
    
    reduce(function(accum, x, y) {
	return accum + x * y;
    }, 0, parallel(coll([2, 3, 4]), asyncCollFromArray([10, 20, 30])),
	   function(res) {
	       assert.equal(res, 2*10+3*20+4*30, "parallel sync/async mix");
	       done();
	   });
});

QUnit.test("collToArray", function(assert) {
    assert.deepEqual(collToArray(coll([])), [], "empty collection");
    assert.deepEqual(collToArray(coll([1, 2, 3, 4])), [1, 2, 3, 4], "sync collection");

    var done = assert.async(2);
    
    collToArray(asyncCollFromArray([1, 2, 3, 4])).result(function(x) {
	assert.deepEqual(x, [1, 2, 3, 4], "async collection");
	done();
    });

    collToArray(asyncCollFromArray([1, 2, 3, 4]), function(x) {
	assert.deepEqual(x, [1, 2, 3, 4], "async collection (callback)");
	done();
    });

    assert.deepEqual(collToArray(parallel(coll([1, 2, 3, 4]), coll(["x", "y", "z"]))),
		     [[1, "x"],
		      [2, "y"],
		      [3, "z"]], "parallel collection");
});

QUnit.test("map", function(assert) {
    assert.deepEqual(collToArray(map(double, coll([]))), [], "empty collection");
    assert.deepEqual(collToArray(map(double, coll([1, 2, 3, 4]))), [2, 4, 6, 8], "collection");
    assert.deepEqual(collToArray(map(div, parallel(coll([5, 10, 30]), coll([1, 2, 6])))), [5, 5, 5], "parallel");
    assert.deepEqual(collToArray(map(double, map(div, parallel(coll([5, 10, 30]), coll([1, 2, 6]))))), [10, 10, 10], "chained");
});

QUnit.test("filter", function(assert) {
    assert.deepEqual(collToArray(filter(even, coll([]))), [], "empty collection");
    assert.deepEqual(collToArray(filter(even, coll([1, 2, 3, 4]))), [2, 4], "collection");
    assert.deepEqual(collToArray(filter(even, map(double, coll([1, 2, 3, 4])))), [2, 4, 6, 8], "order1");
    assert.deepEqual(collToArray(map(double, filter(even, coll([1, 2, 3, 4])))), [4, 8], "order2");
});

QUnit.test("take", function(assert) {
    assert.deepEqual(collToArray(take(0, coll([]))), [], "empty collection, take none");
    assert.deepEqual(collToArray(take(1, coll([]))), [], "empty collection, take 1");
    assert.deepEqual(collToArray(take(0, coll([1, 2, 3]))), [], "collection, take none");
    assert.deepEqual(collToArray(take(10, coll([1, 2, 3]))), [1, 2, 3], "collection, take over");
    assert.deepEqual(collToArray(take(2, coll([1, 2, 3]))), [1, 2], "collection");
    
    assert.deepEqual(collToArray(take(3, map(double, coll([1, 2, 3, 4])))), [2, 4, 6], "order1");
    assert.deepEqual(collToArray(map(double, take(3, coll([1, 2, 3, 4])))), [2, 4, 6], "order2");

    assert.deepEqual(collToArray(parallel(coll([1, 2, 3, 4, 5, 6]),
					  take(3, coll(["x", "y", "z", "w"])))),
		     [[1, "x"], [2, "y"], [3, "z"]]);
});
