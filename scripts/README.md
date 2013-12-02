sstable2json
=========

This is just a simple wrapper around a similar tool to one that cassandra provides.  
You can use it to test aegisthus or to debug futher in your editor. 

Convert uncompressed data to json:
----------------------------------

<pre><code>./sstable2json data/test-uncompressed-ib-1-Data.db
</code></pre>


Convert compressed data to json:
--------------------------------

<pre><code>./sstable2json data/test-compressed-ib-1-Data.db -comp data/test-compressed-ib-1-CompressionInfo.db
</code></pre>

