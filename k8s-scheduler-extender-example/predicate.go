package main

import (
	"k8s.io/api/core/v1"
	schedulerapi "k8s.io/kubernetes/pkg/scheduler/apis/extender/v1"
	"log"
)

type Predicate struct {
	Name string
	Func func(pod v1.Pod, node v1.Node) (bool, error)
}

// func (p Predicate) Handler(args schedulerapi.ExtenderArgs) *schedulerapi.ExtenderFilterResult {.....}
// input args schedulerapi.ExtenderArgs
// output extender filter result
// args.Nodes.Items contains filtered nodes from default scheduler (based on some Criterias)

func (p Predicate) Handler(args schedulerapi.ExtenderArgs) *schedulerapi.ExtenderFilterResult {
	// access the API to list pods

	pod := args.Pod
	canSchedule := make([]v1.Node, 0, len(args.Nodes.Items))
	canNotSchedule := make(map[string]string)

	for _, node := range args.Nodes.Items {
		//TODO: check pods distribution in map for each node
		result, err := p.Func(*pod, node)
		if err != nil {
			canNotSchedule[node.Name] = err.Error()
			log.Printf("info: $$$$$ can not schedule pod on node: %s $$$$$", node.Name)
		} else {
			if result {
				canSchedule = append(canSchedule, node)
			}
		}
	}

	result := schedulerapi.ExtenderFilterResult{
		Nodes: &v1.NodeList{
			Items: canSchedule,
		},
		FailedNodes: canNotSchedule,
		Error:       "",
	}

	return &result
}
