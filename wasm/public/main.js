import init, { run_query } from "./pkg/bogosql_wasm.js";

init();

function onQuery() {
    const query = document.getElementById("query");
    const queryValue = query.value;
    const res = run_query(queryValue);
    const resultElem = document.getElementById("result");
    resultElem.value = res;
}

const button = document.getElementById("runQuery");
button.addEventListener("click", onQuery);
