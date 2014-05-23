require 'helpers'

require 'redis'

$channel = nil
$message = nil

class Redis
  def initialize(options = {})
  end

  def pipelined
    yield self
  end

  def set(key, message)
    $command = :set
    $key = key
    $message = message
  end

  def rpush(key, message)
    $command = :rpush
    $key = key
    $message = message
  end

  def lpush(key, message)
    $command = :lpush
    $key = key
    $message = message
  end

  def sadd(key, message)
    $command = :sadd
    $key = key
    $message = message
  end

  def zadd(key, score, message)
    $command = :zadd
    $key = key
    $score = score
    $message = message
  end

  def expire(key, ttl)
    $expire_key = key
    $ttl = ttl
  end

  def publish(channel, message)
    $channel = channel
    $message = message
  end

  def quit
  end
end

class RedisStoreOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  CONFIG_OMIT_KEY = %[
  ]
  CONFIG_OMIT_SCORE = %[
    key_name a
  ]
  CONFIG1 = %[
    key_name a
    score_name b
  ]
  CONFIG2 = %[
    host 192.168.2.3
    port 9999
    db_number 3
    timeout 7
    fixed_key_name a
    score_name b
  ]
  CONFIG3 = %[
    path /tmp/foo.sock
    fixed_key_name a
    score_name b
  ]

  CONFIG_OMIT_VALUE_NAME = %[
    format_type plain
    store_type string
    key_name   user
  ]
  CONFIG_KEY_VALUE_PATHS = %[
    format_type plain
    store_type string
    key_name   user.name
    value_name stat.attack
    key_expire 3
  ]
  CONFIG_JSON = %[
    format_type json
    store_type string
    key_name   user
  ]
  CONFIG_MSGPACK = %[
    format_type msgpack
    store_type string
    key_name   user
  ]
  CONFIG_LIST_ASC = %[
    format_type plain
    store_type list
    key_name   user
  ]
  CONFIG_LIST_DESC = %[
    format_type plain
    store_type list
    key_name   user
    order      desc
  ]
  CONFIG_SET = %[
    format_type plain
    store_type set
    key_name   user
    order      desc
  ]
  CONFIG_ZSET = %[
    format_type plain
    store_type zset
    key_name   user
    score_name result
  ]

  def create_driver(conf)
    Fluent::Test::BufferedOutputTestDriver.new(Fluent::RedisStoreOutput).configure(conf)
  end

  def test_configure
    # defaults
    d = create_driver(CONFIG1)
    assert_equal "127.0.0.1", d.instance.host
    assert_equal 6379, d.instance.port
    assert_equal nil, d.instance.path
    assert_equal 0, d.instance.db_number
    assert_equal 5.0, d.instance.timeout
    assert_equal 'plain', d.instance.format_type
    assert_equal '', d.instance.key_prefix
    assert_equal '', d.instance.key_suffix
    assert_equal 'zset', d.instance.store_type
    assert_equal 'a', d.instance.key_name
    assert_equal nil, d.instance.fixed_key_name
    assert_equal 'b', d.instance.score_name
    assert_equal '', d.instance.value_name
    assert_equal -1, d.instance.key_expire
    assert_equal -1, d.instance.value_expire
    assert_equal -1, d.instance.value_length
    assert_equal 'asc', d.instance.order

    # host port db
    d = create_driver(CONFIG2)
    assert_equal "192.168.2.3", d.instance.host
    assert_equal 9999, d.instance.port
    assert_equal nil, d.instance.path
    assert_equal 3, d.instance.db_number
    assert_equal 7.0, d.instance.timeout
    assert_equal nil, d.instance.key_name
    assert_equal 'a', d.instance.fixed_key_name

    # path
    d = create_driver(CONFIG3)
    assert_equal "/tmp/foo.sock", d.instance.path
  end

  def test_configure_exception
    assert_raise(Fluent::ConfigError) do
      create_driver(CONFIG_OMIT_KEY)
    end

    assert_raise(Fluent::ConfigError) do
      create_driver(CONFIG_OMIT_SCORE)
    end
  end

#  def test_write
#    d = create_driver(CONFIG1)
#
#    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
#    d.emit({ "foo" => "bar" }, time)
#    d.run
#
#    assert_equal "test", $channel
#    assert_equal(%Q[{"foo":"bar","time":#{time}}], $message)
#  end

  def get_time
    Time.parse("2011-01-02 13:14:15 UTC").to_i
  end

  # it should return whole message
  def test_omit_value_name
    d = create_driver(CONFIG_OMIT_VALUE_NAME)
    message = {
      'user' => 'george',
      'stat' => { 'attack' => 7 }
    }
    $ttl = nil
    d.emit(message, get_time)
    d.run

    assert_equal "george", $key
    assert_equal message, $message
    assert_equal nil, $ttl
  end

  def test_key_value_paths
    d = create_driver(CONFIG_KEY_VALUE_PATHS)
    message = {
      'user' => { 'name' => 'george' },
      'stat' => { 'attack' => 7 }
    }
    $ttl = nil
    d.emit(message, get_time)
    d.run

    assert_equal "george", $key
    assert_equal 7, $message
    assert_equal 3, $ttl
  end

  def test_json
    d = create_driver(CONFIG_JSON)
    message = {
      'user' => 'george',
      'stat' => { 'attack' => 7 }
    }
    $ttl = nil
    d.emit(message, get_time)
    d.run

    assert_equal "george", $key
    assert_equal message.to_json, $message
  end

  def test_msgpack
    d = create_driver(CONFIG_MSGPACK)
    message = {
      'user' => 'george',
      'stat' => { 'attack' => 7 }
    }
    $ttl = nil
    d.emit(message, get_time)
    d.run

    assert_equal "george", $key
    assert_equal message.to_msgpack, $message
  end

  def test_list_asc
    d = create_driver(CONFIG_LIST_ASC)
    message = {
      'user' => 'george',
      'stat' => { 'attack' => 7 }
    }
    d.emit(message, get_time)
    d.run

    assert_equal :rpush, $command
    assert_equal "george", $key
    assert_equal message, message
  end

  def test_list_desc
    d = create_driver(CONFIG_LIST_DESC)
    message = {
      'user' => 'george',
      'stat' => { 'attack' => 7 }
    }
    d.emit(message, get_time)
    d.run

    assert_equal :lpush, $command
    assert_equal "george", $key
    assert_equal message, message
  end

  def test_set
    d = create_driver(CONFIG_SET)
    message = {
      'user' => 'george',
      'stat' => { 'attack' => 7 }
    }
    d.emit(message, get_time)
    d.run

    assert_equal :sadd, $command
    assert_equal "george", $key
    assert_equal message, message
  end

  def test_zset
    d = create_driver(CONFIG_ZSET)
    message = {
      'user' => 'george',
      'stat' => { 'attack' => 7 },
      'result' => 81
    }
    d.emit(message, get_time)
    d.run

    assert_equal :zadd, $command
    assert_equal "george", $key
    assert_equal 81, $score
    assert_equal message, message
  end
end
