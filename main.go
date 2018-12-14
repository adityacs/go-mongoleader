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
var database string
var hostname string

func main() {
	if err := parseArguments(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	info := &mgo.DialInfo{
		Addrs:    []string{hostname},
		Database: database,
		Username: "",
		Password: "",
		Timeout:  30 * time.Second,
	}
	sess, err := mgo.DialWithInfo(info)
	if err != nil {
		log.Errorf("Error in getting mongo connection: %s", err)
	}
	session = sess

	for {
		leader, err := getLeader()
		if err != nil {
			log.Errorf("Error in fetching current leader: %s", err)
			log.Info("No leader found")
		}

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

		} else {
			log.Infof("Current leader is: %s", leader.Name)
		}
		time.Sleep(1 * time.Minute)

	}

}

func parseArguments() error {
	flag.StringVar(&name, "name", "", "name for this node")
	flag.StringVar(&database, "database", "", "Database name to connect")
	flag.StringVar(&hostname, "hostname", "", "MongoDB Hostname")

	flag.Parse()

	if name == "" {
		return errors.New("required argument name not provided")
	}

	return nil
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
	err := s.DB("leader").C("lock").Find(bson.M{}).Sort("-updated").One(&leader)
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
