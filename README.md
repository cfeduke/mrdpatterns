# Mr. D. Patterns

Err uh... MapReduce Design Patterns.

As I work through the [ MapReduce Design Patterns ](http://shop.oreilly.com/product/0636920025122.do) book I need a place to stash my source code. This is it.

I stayed moderately true to the examples, with some re-arrangement here and there. Most notably the MRDPUtils#transformXmlToMap performs a StringEscapeUtils#unescapeHtml within itself rather than separately in any mapper that needs that functionality.

