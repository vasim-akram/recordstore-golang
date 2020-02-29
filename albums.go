package main

import (
	"errors"
	"log"

	"github.com/gomodule/redigo/redis"
)

var pool *redis.Pool

// ErrNoAlbum msg
var ErrNoAlbum = errors.New("no album found")

// Album struct
type Album struct {
	Title  string  `redis:"title"`
	Artist string  `redis:"artist"`
	Price  float64 `redis:"price"`
	Likes  int     `redis:"likes"`
}

// FindAlbum by id
func FindAlbum(id string) (*Album, error) {

	conn := pool.Get()

	defer conn.Close()

	values, err := redis.Values(conn.Do("HGETALL", "album:"+id))
	if err != nil {
		return nil, err
	} else if len(values) == 0 {
		return nil, ErrNoAlbum
	}

	var album Album
	err = redis.ScanStruct(values, &album)
	if err != nil {
		return nil, err
	}

	return &album, nil
}

// IncrementLikes by id
func IncrementLikes(id string) error {
	conn := pool.Get()
	defer conn.Close()

	// check album exists
	exists, err := redis.Int(conn.Do("EXISTS", "album:"+id))
	if err != nil {
		return err
	} else if exists == 0 {
		return ErrNoAlbum
	}

	// start redis transaction
	err = conn.Send("MULTI")
	if err != nil {
		return err
	}

	// inc album likes
	err = conn.Send("HINCRBY", "album:"+id, "likes", 1)
	if err != nil {
		return err
	}

	// inc sorted set likes
	err = conn.Send("ZINCRBY", "likes", 1, id)
	if err != nil {
		return err
	}

	// Execute both command as transaction
	_, err = conn.Do("EXEC")
	if err != nil {
		return err
	}

	return nil
}

// FindTopThree album (popular)
func FindTopThree() ([]*Album, error) {
	conn := pool.Get()
	defer conn.Close()

	for {
		// step - 1
		_, err := conn.Do("WATCH", "likes")
		if err != nil {
			return nil, err
		}

		// step - 2
		ids, err := redis.Strings(conn.Do("ZREVRANGE", "likes", 0, 2))
		if err != nil {
			return nil, err
		}

		// step - 3
		err = conn.Send("MULTI")
		if err != nil {
			return nil, err
		}

		for _, id := range ids {
			err := conn.Send("HGETALL", "album:"+id)
			if err != nil {
				return nil, err
			}
		}

		replies, err := redis.Values(conn.Do("EXEC"))
		if err == redis.ErrNil {
			log.Println("try again")
			continue
		} else if err != nil {
			return nil, err
		}

		albums := make([]*Album, 3)

		for i, reply := range replies {
			var album Album
			err = redis.ScanStruct(reply.([]interface{}), &album)
			if err != nil {
				return nil, err
			}
			albums[i] = &album
		}
		return albums, nil
	}
}
