<?php

function rest_call($url, $timeout = 4) {
    static $handle = NULL; // reuse connections to the same server
    if ($handle === NULL) {
        $handle = curl_init();
    }

    curl_setopt($handle, CURLOPT_URL, $url);
    curl_setopt($handle, CURLOPT_HTTPHEADER, array(
        'Accept: application/json',
        'Content-Type: application/json',
    ));
    curl_setopt($handle, CURLOPT_RETURNTRANSFER, true);
    curl_setopt($handle, CURLOPT_SSL_VERIFYHOST, false);
    curl_setopt($handle, CURLOPT_SSL_VERIFYPEER, false);
    curl_setopt($handle, CURLOPT_HTTP_TRANSFER_DECODING, 1); // read chunked
    curl_setopt($handle, CURLOPT_TIMEOUT, $timeout); // in sec
    $ret = curl_exec($handle);
    $errno = curl_errno($handle);
    if ($errno > 0) {
        // e,g timeout
        $ret = array();
        $ret['_err'] = $errno;
    }

    //curl_close($handle);
    return $ret;
}

// sub
for ($i=0; $i<5; $i++) {
	$ret = rest_call("http://localhost:9192/topics/v1/foobar/mygroup1?limit=5");
	echo $i, " ", $ret, "\n";
}
