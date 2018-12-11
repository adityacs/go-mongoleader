package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	log "github.com/sirupsen/logrus"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type Leader struct {
	Name    string    `json:"name"`
	Updated time.Time `json:"updated"`
}

var session *mgo.Session
var name string

func main() {

	//var ttl int64
	//var wait int64
	if err := parseArguments(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	info := &mgo.DialInfo{
		Addrs:    []string{"127.0.0.1"},
		Database: "leader",
		Username: "",
		Password: "",
		Timeout:  30 * time.Second,
	}
	sess, err := mgo.DialWithInfo(info)
	if err != nil {
		log.Errorf("Error in getting mongo connection: %s", err)
	}
	session = sess
	log.Info(session)
	//session.DB("leader").C("lock").Create
	// val, _ := randomHex(32)
	// fmt.Println(val)
	// index := mgo.Index{
	// 	Key:         []string{"updated"},
	// 	ExpireAfter: 2 * time.Minute,
	// }
	// err = session.DB("leader").C("lock").EnsureIndex(index)
	// if err != nil {
	// 	log.Errorf("Error in creating index: %s", err)
	// }

	//go elect()
	for {
		leader, err := getLeader()
		if err != nil {
			log.Errorf("Error in fetching current leader: %s", err)
		}
		log.Info(name)
		if leader.Name == "" || leader.Updated.Before(time.Now().Add(-2*time.Minute)) {
			leaderName, err := acquireLeader(name)
			if err != nil {
				log.Errorf("Error while acquiring leader: %s", err)
			}
			log.Infof("New Leader is: %s", leaderName)
		} else if leader.Name == name {
			err = updateLeader(leader.Name)
			if err != nil {
				log.Errorf("Updating leader failed: %s", err)
			}
			log.Infof("Leader updated: %s", leader.Name)
			time.Sleep(1 * time.Minute)
		} else {
			log.Infof("Leader is: %s", leader.Name)
			time.Sleep(1 * time.Minute)
		}

	}

}

func parseArguments() error {
	flag.StringVar(&name, "name", "", "name for this node")

	flag.Parse()

	if name == "" {
		return errors.New("required argument name not provided")
	}

	return nil
}

func elect() {
	for {
		time.Sleep(1 * time.Minute)
		leader, err := getLeader()
		if err != nil {
			log.Errorf("Error in fetching current leader: %s", err)
		}

		if leader.Name == "" || leader.Updated.Before(time.Now().Add(-2*time.Minute)) {
			leaderName, err := acquireLeader(name)
			if err != nil {
				log.Errorf("Error while acquiring leader: %s", err)
			}
			log.Infof("Leader is: %s", leaderName)
		} else {
			err = updateLeader(leader.Name)
			if err != nil {
				log.Errorf("Updating leader failed: %s", err)
			}
			log.Infof("Leader updated: %s", leader.Name)
		}

	}
}

func acquireLeader(name string) (string, error) {
	s := session.Copy()
	defer s.Close()
	l := Leader{
		Name:    name,
		Updated: time.Now(),
	}
	if err := s.DB("leader").C("lock").Insert(l); err != nil {
		return "", err
	}
	return l.Name, nil
}

func getLeader() (Leader, error) {
	s := session.Copy()
	defer s.Close()
	var leader Leader
	err := s.DB("leader").C("lock").Find(bson.M{}).Sort("updated").One(&leader)
	if err != nil {
		return leader, err
	}
	return leader, nil
}

func updateLeader(name string) error {
	s := session.Copy()
	defer s.Close()
	l := Leader{
		Name:    name,
		Updated: time.Now(),
	}
	if err := s.DB("leader").C("lock").Update(bson.M{"name": name}, l); err != nil {
		return err
	}
	return nil
}

// func randomHex(n int) (string, error) {
// 	bytes := make([]byte, n)
// 	if _, err := rand.Read(bytes); err != nil {
// 		return "", err
// 	}
// 	return hex.EncodeToString(bytes), nil
// }
