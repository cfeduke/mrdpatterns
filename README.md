# Mr. D. Patterns

Err uh... MapReduce Design Patterns.

As I work through the [MapReduce Design Patterns](http://shop.oreilly.com/product/0636920025122.do) book I need a place to stash my source code. This is it.

I stayed moderately true to the examples, with some re-arrangement here and there. Most notably the MRDPUtils#transformXmlToMap performs a StringEscapeUtils#unescapeHtml within itself rather than separately in any mapper that needs that functionality.

## To use

    $ mvn package

I've placed a bunch of scripts in the ./bin/ directory. These make a few terrible assumptions about your environment. You can change ./bin/env.sh to be more accomodating.

1. There is a $HADOOP_HOME, even though its deprecated
2. The $DATADIR is mapped to $DATADIR=/Users/$USER/Downloads/stack-overflow-dump-2009-09
3. You have the [CC data dump](http://www.clearbits.net/creators/146-stack-exchange-data-dump) from StackOverflow (I used 2009 because its smallish, you should be able to use any year)
4. The launch scripts assume single node mode

Make sure Hadoop is running ($HADOOP_HOME/bin/start-all.sh) and execute the script of your choice.

