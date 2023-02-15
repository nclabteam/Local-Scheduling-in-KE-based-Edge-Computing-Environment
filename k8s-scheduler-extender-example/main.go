package main

import (
	"encoding/json"
	"fmt"
	"github.com/comail/colog"
	"github.com/julienschmidt/httprouter"
	"gopkg.in/yaml.v2"
	v1autoscaling "k8s.io/api/autoscaling/v1"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog"
	"k8s.io/kubernetes/pkg/apis/autoscaling"
	schedulerapi "k8s.io/kubernetes/pkg/scheduler/apis/extender/v1"
	"log"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	versionPath      = "/version"
	apiPrefix        = "/scheduler"
	bindPath         = apiPrefix + "/bind"
	preemptionPath   = apiPrefix + "/preemption"
	predicatesPrefix = apiPrefix + "/predicates"
	prioritiesPrefix = apiPrefix + "/priorities"
	EPS = 0.1)

type safeIntValueMap struct {
	mu sync.RWMutex
	intValueMap map[string]int
}

type safeFloatValueMap struct {
	mu sync.RWMutex
	stringFloat64Map map[string]float64
}

type safeAppTrafficInfo struct {
	mu sync.RWMutex
	appTrafficInfo map[string]safeFloatValueMap
}

type safePodsDistributionMap struct {
	mu sync.RWMutex
	podsDistributionRatioMap map[string]safeIntValueMap
}

type safeUpdatedCheckMap struct {
	mu sync.RWMutex
	isMapUpdated map[string]bool
}

type Config struct {
	Application struct {
		Name        string `yaml:"name"`
		NameSpace   string `yaml:"namespace"`
		DefaultPods string `yaml:"defaultPods"`
	}
	K8sConfigPath struct {
		Path string `yaml:"path"`
	}
}

var (
	version string // injected via ldflags at build time
	//Phuc
	sHpaObject                    = "HorizontalPodAutoscaler"
	//example message HorizontalPodAutoscaler--app-example1--New size: 7; reason: cpu resource utilization (percentage of request) above target
	bInit                         = false
	bCalculating				  = false
	neighborsInfo map[string]map[string]float64
	node1Neighbors map[string]float64
	node2Neighbors map[string]float64
	node3Neighbors map[string]float64
	//podsDistributionRatioMap safeIntValueMap
	//nodesTrafficMap safeFloatValueMap
	podsDistributionInfo safePodsDistributionMap
	appTrafficInfo       safeAppTrafficInfo
	appUpdateInfoMap     safeUpdatedCheckMap
	//global variable
	customConfig Config
	config *rest.Config
	clientset *kubernetes.Clientset
	appPodOptions metav1.ListOptions
	defaultApplicationPods int
	start time.Time
	// end Phuc

	// Create Filter, Priority, .. for scheduling
	TruePredicate Predicate

	ZeroPriority Prioritize

	NoBind Bind

	EchoPreemption Preemption
	podsDis map[string]int
)

func StringToLevel(levelStr string) colog.Level {
	switch level := strings.ToUpper(levelStr); level {
	case "TRACE":
		return colog.LTrace
	case "DEBUG":
		return colog.LDebug
	case "INFO":
		return colog.LInfo
	case "WARNING":
		return colog.LWarning
	case "ERROR":
		return colog.LError
	case "ALERT":
		return colog.LAlert
	default:
		log.Printf("warning: LOG_LEVEL=\"%s\" is empty or invalid, fallling back to \"INFO\".\n", level)
		return colog.LInfo
	}
}

func main() {

	// Init configs
	podsDis = make(map[string]int)
	podsDis["edge1"] = 6
        podsDis["edge2"] = 6
        podsDis["edge3"] = 6
	initConfigs()
	start = time.Now()
	defaultApplicationPods, _ = strconv.Atoi(customConfig.Application.DefaultPods)
	//Init test set
	bInit = true
	//TODO we must update neighbors for each node in another func
	//edge1 neighbors
	node1Neighbors = make(map[string]float64)
	node1Neighbors["edge2"] = 5
	node1Neighbors["edge3"] = 20
	log.Printf("node1Neighbors len = %d", len(node1Neighbors))
	//edge2 neighbors
	node2Neighbors = make(map[string]float64)
	node2Neighbors["edge1"] = 5
	node2Neighbors["edge3"] = 10
	//edge3 neighbors
	node3Neighbors = make(map[string]float64)
	node3Neighbors["edge2"] = 10
	node3Neighbors["edge1"] = 20

	neighborsInfo = make(map[string]map[string]float64)
	neighborsInfo["edge1"] = node1Neighbors
	neighborsInfo["edge2"] = node2Neighbors
	neighborsInfo["edge3"] = node3Neighbors
	appTrafficInfo = safeAppTrafficInfo{
		mu:             sync.RWMutex{},
		appTrafficInfo: make(map[string]safeFloatValueMap),
	}
	podsDistributionInfo = safePodsDistributionMap{
		mu:                       sync.RWMutex{},
		podsDistributionRatioMap: make(map[string]safeIntValueMap),
	}
	appUpdateInfoMap = safeUpdatedCheckMap{
		mu:           sync.RWMutex{},
		isMapUpdated: make(map[string]bool),
	}

	//podsDis := make(map[string]int)
	//podsDis["edge1"] = 3
	//podsDis["edge2"] = 3
	//podsDis["edge3"] = 2

	colog.SetDefaultLevel(colog.LInfo)
	colog.SetMinLevel(colog.LInfo)
	colog.SetFormatter(&colog.StdFormatter{
		Colors: true,
		Flag:   log.Ldate | log.Ltime | log.Lshortfile,
	})
	colog.Register()
	level := StringToLevel(os.Getenv("LOG_LEVEL"))
	log.Print("Log level was set to ", strings.ToUpper(level.String()))
	colog.SetMinLevel(level)

	router := httprouter.New()
	AddVersion(router)

	predicates := []Predicate{TruePredicate}
	for _, p := range predicates {
		AddPredicate(router, p)
	}
	priorities := []Prioritize{ZeroPriority}
	for _, p := range priorities {
		AddPrioritize(router, p)
	}
	AddBind(router, NoBind)

	// watch used to get all events
	watch, err := clientset.CoreV1().Events(customConfig.Application.NameSpace).Watch(metav1.ListOptions{})
	if err != nil {
		log.Fatal(err.Error())
	}
	ch := watch.ResultChan()
	// this goroutine is used to parse all the events that generated by HPA object.
	go func() {
		for event := range ch {
			e, _ := event.Object.(*v1.Event)
			if (start.Sub(e.FirstTimestamp.Time)).Minutes() > 1 {
				// This if statement is used to ignore old events
				continue
			}
			if strings.Contains(e.Message, "old size") && strings.Contains(e.Message, "new size") {
				message := e.Message
				oldSizeMessagePart := message[strings.Index(message, "old size: "):strings.Index(message, ", new size")]
				oldSize, _ := strconv.Atoi(strings.ReplaceAll(oldSizeMessagePart, "old size: ", ""))
				newSizeMessagePart := message[strings.Index(message, "new size: "):strings.Index(message, ", reason")]
				newSize, _ := strconv.Atoi(strings.ReplaceAll(newSizeMessagePart, "new size: ", ""))
				if newSize < oldSize {
					continue
				}
				appName := e.InvolvedObject.Name
				appUpdateInfoMap.mu.Lock()
				appUpdateInfoMap.isMapUpdated[appName] = false
				appUpdateInfoMap.mu.Unlock()
				if _, exist := appTrafficInfo.appTrafficInfo[appName]; !exist {
					appTrafficInfo.mu.Lock()
					podsDistributionInfo.mu.Lock()
					appTrafficInfo.appTrafficInfo[appName] = nodesTrafficMapInit()
					podsDistributionInfo.podsDistributionRatioMap[appName] = nodesPodsDistributionMapInit()
					appTrafficInfo.mu.Unlock()
					podsDistributionInfo.mu.Unlock()
				}
				log.Printf("goroutine id %d", goid())
				log.Print("============================")
				log.Print("====HPA Upscale happends====")
				log.Printf("=======New pod = %d=========", newSize - oldSize)
				log.Print("============================")
				updateNodesTraffic(appTrafficInfo.appTrafficInfo[appName], appName)
				podsDistributionInfo.mu.Lock()
				calculatePodsDistribution(newSize - oldSize, appTrafficInfo.appTrafficInfo[appName], podsDistributionInfo.podsDistributionRatioMap[appName])
				podsDistributionInfo.mu.Unlock()
				appUpdateInfoMap.mu.Lock()
				appUpdateInfoMap.isMapUpdated[appName] = true
				appUpdateInfoMap.mu.Unlock()
			}
		}

	}()

	time.Sleep(2 * time.Second)

	log.Print("info: server starting on localhost port :8888")
	bInit = false // Prevent old events affect
	if err := http.ListenAndServe("localhost:8888", router); err != nil {
		log.Fatal(err)
	}

}

func calculatePodsDistribution(iNumberOfNewPodScheduledByHPA int, nodesTrafficMap safeFloatValueMap, podsDistributionRatioMap safeIntValueMap) {
	log.Print("(Calculation Begin) Calculating pods distribution")
	// We need to update latest traffic info for all nodes before calculation
	sumTrafficFromMap := nodesTrafficMap.getValuesSum()
	allNodesHasSameTraffic := false
	if sumTrafficFromMap - EPS < 0 {
		log.Printf("Sum < EPS")
		nodesTrafficMap.equalizeValuesTo1()
		sumTrafficFromMap = nodesTrafficMap.getValuesSum()
		allNodesHasSameTraffic = true
	}
	sortedNodesNameBasedOnTraffic := getSortedMapKeysByValueDesc(nodesTrafficMap.getFloat64ValueMap())
	podsDistributionRatioMap.mu.Lock()
	iNumberOfNewPodScheduledByHPAClone := iNumberOfNewPodScheduledByHPA
	for iNumberOfNewPodScheduledByHPAClone > 0 {
		for i, nodeName := range sortedNodesNameBasedOnTraffic {
			if iNumberOfNewPodScheduledByHPAClone == 0 {
				break
			}
			numberOfPods := int(math.RoundToEven(float64(iNumberOfNewPodScheduledByHPA) * (nodesTrafficMap.getFloat64Value(nodeName) / sumTrafficFromMap)))
			if numberOfPods > iNumberOfNewPodScheduledByHPAClone {
				numberOfPods = iNumberOfNewPodScheduledByHPAClone
			}
			if numberOfPods == 0 && i == 0 {
				allNodesHasSameTraffic = true
			}
			if allNodesHasSameTraffic == true {
				numberOfPods = 1
			}
			currentPodsOnNode := podsDistributionRatioMap.getInValue(nodeName)
			podsDistributionRatioMap.setIntValue(nodeName, currentPodsOnNode + numberOfPods)
			iNumberOfNewPodScheduledByHPAClone = iNumberOfNewPodScheduledByHPAClone - numberOfPods
		}
	}
	log.Print("(Calculation Done) Pods distribution calculated")
	podsDistributionRatioMap.printPodsDistributionInfo()
	podsDistributionRatioMap.mu.Unlock()
}

func getAllRunningAppPod () int {
	// get the pod list
	podList, _ := clientset.CoreV1().Pods(customConfig.Application.NameSpace).List(metav1.ListOptions{
		LabelSelector: "app=" + customConfig.Application.Name,
	})
	count := 0
	for _, pod := range podList.Items {
		if pod.Status.Phase == v1.PodRunning {
			count++
		}
	}
	// List() returns a pointer to slice, derefernce it, before iterating
	log.Printf("info:func GetAllActiveAppPod() => Current pods in cluster = %d", count)

	return count
}

func initConfigs () {

	log.Print("Initialize configuration")
	//Read configuration file
	log.Print("Read configuration file")
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	log.Printf("Current dir = %s", dir)
	if err != nil {
		log.Fatal(err)
	}
	f, err := os.Open(dir + "/config.yaml")
	if err != nil {
		log.Fatal(err)
	}

	decoder := yaml.NewDecoder(f)
	err = decoder.Decode(&customConfig)
	if err != nil {
		log.Fatal(err)
	}

	if errWhenCloseFile := f.Close(); errWhenCloseFile != nil {
		log.Fatal(errWhenCloseFile)
	}
	log.Printf("config path %s", customConfig.K8sConfigPath.Path)
	// initConfigs
	config , _ = clientcmd.BuildConfigFromFlags("", customConfig.K8sConfigPath.Path)
	if config == nil {
		log.Print("Config is nil")
	}
	clientset, _ = kubernetes.NewForConfig(config)

	appPodOptions = metav1.ListOptions{
		LabelSelector: "app=" + customConfig.Application.Name,
	}
	initScheduleRules()

}

func initScheduleRules () {

	log.Print("Adding schedule rules")
	TruePredicate = Predicate{
		Name: "always_true",
		Func: func(pod v1.Pod, node v1.Node) (bool, error) {
			return true, nil
		},
	}

	ZeroPriority = Prioritize{
		Name: "traffic_neighbors_scoring",
		Func: func(pod v1.Pod, nodes []v1.Node) (*schedulerapi.HostPriorityList, error) {
			var priorityList schedulerapi.HostPriorityList
			isScheduled := false
			priorityList = make([]schedulerapi.HostPriority, len(nodes))
			//missingLabelErr := "Application " + pod.Name + "does not have app label"
			bMissingAppLabel := false
			labelMap := pod.ObjectMeta.Labels
			if labelMap == nil {
				bMissingAppLabel = true
			}
			if _, exist := labelMap["app"]; !exist {
				bMissingAppLabel = true
			}
			appName := labelMap["app"]
			isHPAReady := false
			hpa, err := clientset.AutoscalingV1().HorizontalPodAutoscalers("default").Get(appName, metav1.GetOptions{})
			if err == nil {
				// The below code is used to check that hpa is ready or not => If it is not ready, it means that all pods pass to this extender prioritize is init pods
				var hpaConditions []v1autoscaling.HorizontalPodAutoscalerCondition
				if err := json.Unmarshal([]byte(hpa.ObjectMeta.Annotations[autoscaling.HorizontalPodAutoscalerConditionsAnnotation]), &hpaConditions); err != nil {
					panic("Cannot get hpa conditions")
				}
				for _, condition := range hpaConditions {
					if condition.Type == v1autoscaling.ScalingActive {
						if condition.Status == "True" {
							isHPAReady = true
							log.Printf("### HPA %s is ready", hpa.Name)
						}
					}
				}
			}
			if strings.Contains(pod.Name, appName) && isHPAReady == true && !bMissingAppLabel && false {
				isAppNameExistInPodsDisMap := false
				isAppNameExistInUpdateInfoMap := false
				podsDistributionInfo.mu.RLock()
				_, isAppNameExistInPodsDisMap = podsDistributionInfo.podsDistributionRatioMap[appName]
				podsDistributionInfo.mu.RUnlock()
				appUpdateInfoMap.mu.RLock()
				_, isAppNameExistInUpdateInfoMap = appUpdateInfoMap.isMapUpdated[appName]
				bIsMapUpdated := appUpdateInfoMap.isMapUpdated[appName]
				appUpdateInfoMap.mu.RUnlock()
				bLog1Time := true
				for !isAppNameExistInPodsDisMap || !isAppNameExistInUpdateInfoMap || !bIsMapUpdated {
					if bLog1Time {
						log.Printf("Wait for all maps to be updated ...")
						bLog1Time = false
					}
					podsDistributionInfo.mu.RLock()
					_, isAppNameExistInPodsDisMap = podsDistributionInfo.podsDistributionRatioMap[appName]
					podsDistributionInfo.mu.RUnlock()
					appUpdateInfoMap.mu.RLock()
					_, isAppNameExistInUpdateInfoMap = appUpdateInfoMap.isMapUpdated[appName]
					bIsMapUpdated = appUpdateInfoMap.isMapUpdated[appName]
					appUpdateInfoMap.mu.RUnlock()
				}
				log.Printf("All Maps are updated")
				// Use for loop here to make sure that no anynomous host prioritize exist
				for !isScheduled {
					podsDistributionInfo.mu.RLock()
					sortedNodeNameBasedOnPodsValue := getSortedMapKeysByPodsValueDesc(podsDistributionInfo.podsDistributionRatioMap[appName].intValueMap)
					podsDistributionInfo.mu.RUnlock()
					for _, nodeName := range sortedNodeNameBasedOnPodsValue {
						podsDistributionInfo.mu.RLock()
						podsOnNode := podsDistributionInfo.podsDistributionRatioMap[appName].intValueMap[nodeName]//podsDistributionRatioMap.getInValue(nodeName)
						podsDistributionInfo.mu.RUnlock()
						if podsOnNode == 0 {
							continue
						}
						log.Print("Start to schedule 1 pod")
						if nodeExistOnList(nodeName, nodes) {
							iMaxScore := len(nodes) * 10
							iIndex := 0
							priorityList[iIndex] = schedulerapi.HostPriority{
								Host:  nodeName,
								Score: int64(iMaxScore),
							}
							log.Printf("Node %s has score = %d", nodeName, iMaxScore)
							iIndex += 1
							iMaxScore = iMaxScore - 10
							sortedNodeNameBasedOnDelayValue := getSortedMapKeysByValueAsc(neighborsInfo[nodeName])
							for _, neighbors := range sortedNodeNameBasedOnDelayValue {
								if nodeExistOnList(neighbors, nodes) {
									priorityList[iIndex] = schedulerapi.HostPriority{
										Host:  neighbors,
										Score: int64(iMaxScore),
									}
									log.Printf("Node %s has score = %d", neighbors, iMaxScore)
									iIndex++
									iMaxScore = iMaxScore - 10
								}
							}
							isScheduled = true
							podsDistributionInfo.mu.Lock()
							oldPodsValueOnNode := podsDistributionInfo.podsDistributionRatioMap[appName].intValueMap[nodeName]//podsDistributionRatioMap.getInValue(nodeName)
							podsDistributionInfo.podsDistributionRatioMap[appName].intValueMap[nodeName] = oldPodsValueOnNode - 1
							podsDistributionInfo.mu.Unlock()
							break
						} else {
							// Iterate all neighbors and give them score
							iMaxScore := len(nodes) * 10
							iIndex := 0
							sortedNodeNameBasedOnDelayValue := getSortedMapKeysByValueAsc(neighborsInfo[nodeName])
							for _, neighbors := range sortedNodeNameBasedOnDelayValue {
								if nodeExistOnList(neighbors, nodes) {
									priorityList[iIndex] = schedulerapi.HostPriority{
										Host:  neighbors,
										Score: int64(iMaxScore),
									}
									iIndex++
									iMaxScore = iMaxScore - 10
								}
							}
							isScheduled = true
							podsDistributionInfo.mu.Lock()
							oldPodsValueOnNode := podsDistributionInfo.podsDistributionRatioMap[appName].intValueMap[nodeName]
							podsDistributionInfo.podsDistributionRatioMap[appName].intValueMap[nodeName] = oldPodsValueOnNode - 1
							podsDistributionInfo.mu.Unlock()
							break
						}
					}
				}
			} else {
				log.Printf("No Prioritize schedule")
				iMaxScore := len(nodes) * 10
				log.Printf("Len of podsDis map = %d", len(podsDis))
				for k, v := range podsDis {
					if v == 0 {
						log.Print("Hello")
						continue
					}
					log.Printf("k = %s", k)
					for i, node := range nodes {
						log.Printf("i = %d", i)
						log.Printf("Filtered: node name = %s", node.Name)
						if node.Name == k {
							priorityList[i] = schedulerapi.HostPriority{
								Host:  node.Name,
								Score: int64(iMaxScore),
							}
						} else {
							priorityList[i] = schedulerapi.HostPriority{
								Host:  node.Name,
								Score: 0,
							}
						}
					}
					podsDis[k] = podsDis[k] - 1
					return &priorityList, nil
				}
				// If no pods will be scheduled by HPA => we do not need to care about them
				//for i, node := range nodes {
				//	tempHostPriority := schedulerapi.HostPriority{}
				//	tempHostPriority.Host = node.Name
				//	priorityList[i] = tempHostPriority
				//}
			}
			if _, exist := podsDistributionInfo.podsDistributionRatioMap[appName]; exist {
				isAllNodesRemainZeroPod := true
				for _, v := range podsDistributionInfo.podsDistributionRatioMap[appName].intValueMap {
					if v != 0 {
						isAllNodesRemainZeroPod = false
						break
					}
				}
				if isAllNodesRemainZeroPod {
					appUpdateInfoMap.mu.Lock()
					appUpdateInfoMap.isMapUpdated[appName] = false
					appUpdateInfoMap.mu.Unlock()
				}
			}

			return &priorityList, nil
		},
	}

	NoBind = Bind{
		Func: func(podName string, podNamespace string, podUID types.UID, node string) error {
			return fmt.Errorf("This extender doesn't support Bind.  Please make 'BindVerb' be empty in your ExtenderConfig.")
		},
	}

	EchoPreemption = Preemption{
		Func: func(_ v1.Pod, _ map[string]*schedulerapi.Victims, nodeNameToMetaVictims map[string]*schedulerapi.MetaVictims, ) map[string]*schedulerapi.MetaVictims {
			return nodeNameToMetaVictims
		},
	}

}

func getNodesTrafficInfoFromEndpoints(workerNodeName string, appName string) float64 {
	realEPName := ""
	endpoints, _ := clientset.CoreV1().Endpoints("default").List(metav1.ListOptions{})
	for _, ep := range endpoints.Items {
		if strings.Contains(ep.Name, workerNodeName) && strings.Contains(ep.Name, appName) {
			realEPName = ep.Name
			break
		}
	}
	if realEPName == "" {
		klog.Infof("EP name = empty")
		return 0
	}
	realEP, _ := clientset.CoreV1().Endpoints("default").Get(realEPName, metav1.GetOptions{})
	annotation := realEP.ObjectMeta.Annotations
	if annotation == nil {
		klog.Infof("EP %s does not have traffic info annotations", realEPName)
		return 0
	}
	curNumRequests, _ := strconv.Atoi(annotation["numRequests"])
	klog.Infof("CurrentNumRequests = %d", curNumRequests)
	oldNumRequests, _ := strconv.Atoi(annotation["oldNumRequests"])
	klog.Infof("oldNumRequests = %d", oldNumRequests)
	trafficValue := float64(curNumRequests - oldNumRequests)
	klog.Infof("Traffic value from ep %s = %f", realEPName, trafficValue)
	annotation["reset"] = "true"

	return trafficValue
}

func updateNodesTraffic(nodesTrafficMap safeFloatValueMap, appName string) {
	//TODO update nodes traffic here by getting information from endpoint on each node
	// Get all worker nodes
	//nodesTrafficMap.mu.Lock()
	log.Print("(Update begin) Updating nodes traffic")
	workerNodes, _ := clientset.CoreV1().Nodes().List(metav1.ListOptions{
		LabelSelector: "node-role.kubernetes.io/worker=true",
	})
	// Get traffic for all worker nodes from EP
	for _, workerNode := range workerNodes.Items {
		nodesTrafficMap.setFloatValue(workerNode.Name, getNodesTrafficInfoFromEndpoints(workerNode.Name, appName))
	}
	log.Print("(Update Done) Updated nodes traffic")
	nodesTrafficMap.printNodesTrafficInfo()
	//nodesTrafficMap.mu.Unlock()
}

// This func is used to sort map keys by their values
func getSortedMapKeysByValueAsc(inputMap map[string]float64) []string {
	mapKeys := make([]string, 0, len(inputMap))
	for key := range inputMap {
		mapKeys = append(mapKeys, key)
	}
	sort.Slice(mapKeys, func(i, j int) bool {
		return inputMap[mapKeys[i]] < inputMap[mapKeys[j]]
	})

	klog.Info("Neighbors name sorted by delay asc")
	for i, key := range mapKeys {
		klog.Infof("- Index %d: %s", i, key)
	}

	return mapKeys
}

// This func is used to sort map keys by their values
func getSortedMapKeysByPodsValueDesc(inputMap map[string]int) []string {
	mapKeys := make([]string, 0, len(inputMap))
	for key := range inputMap {
		mapKeys = append(mapKeys, key)
	}
	sort.Slice(mapKeys, func(i, j int) bool {
		return inputMap[mapKeys[i]] > inputMap[mapKeys[j]]
	})

	return mapKeys
}

func getSortedMapKeysByValueDesc(inputMap map[string]float64) []string {
	mapKeys := make([]string, 0, len(inputMap))
	for key := range inputMap {
		mapKeys = append(mapKeys, key)
	}
	sort.Slice(mapKeys, func(i, j int) bool {
		return inputMap[mapKeys[i]] > inputMap[mapKeys[j]]
	})

	klog.Info("Sorted keys of traffic map")
	for i, key := range mapKeys {
		klog.Infof("- Index %d: %s", i, key)
	}

	return mapKeys
}

func nodesTrafficMapInit() safeFloatValueMap {
	nodesTrafficMap := safeFloatValueMap{stringFloat64Map: make(map[string]float64)}
	workerNodes, _ := clientset.CoreV1().Nodes().List(metav1.ListOptions{
		LabelSelector: "node-role.kubernetes.io/worker=true",
	})
	// Get traffic for all worker nodes from EP
	for _, workerNode := range workerNodes.Items {
		nodesTrafficMap.setFloatValue(workerNode.Name, 0.0)
	}
	return nodesTrafficMap
}

func nodesPodsDistributionMapInit() safeIntValueMap {
	nodesPodsDistributionMap := safeIntValueMap{intValueMap: make(map[string]int)}
	workerNodes, _ := clientset.CoreV1().Nodes().List(metav1.ListOptions{
		LabelSelector: "node-role.kubernetes.io/worker=true",
	})
	// Get traffic for all worker nodes from EP
	for _, workerNode := range workerNodes.Items {
		nodesPodsDistributionMap.setIntValue(workerNode.Name, 0)
	}
	return nodesPodsDistributionMap
}

func calTotalFromMapValues(trafficMap map[string]float64) float64 {
	result := 0.0
	for _, value := range trafficMap {
		result += value
	}
	return result

}

func nodeExistOnList(a string, list []v1.Node) bool {
	for _, b := range list {
		if b.Name == a {
			return true
		}
	}
	return false
}

func equalizeNodeTraffic(cloneNodesTrafficMap map[string]float64){
	for nodeName, _ := range cloneNodesTrafficMap {
		cloneNodesTrafficMap[nodeName] = 1.0
	}
}

func updateOldTrafficForEPs() {
	workerNodesName := make([]string, 0)
	EPsName := make([]string, 0)
	workerNodes, _ := clientset.CoreV1().Nodes().List(metav1.ListOptions{
		LabelSelector: "node-role.kubernetes.io/worker=true",
	})
	for _, worker := range workerNodes.Items {
		workerNodesName = append(workerNodesName, worker.Name)
	}
	if len(workerNodesName) == 0 {
		log.Printf("Worker nodes name list is empty")
	}
	endpoints, _ := clientset.CoreV1().Endpoints("default").List(metav1.ListOptions{})
	epPrefix := "-" + customConfig.Application.Name
	for _, ep := range endpoints.Items {
		if !strings.Contains(ep.Name, customConfig.Application.Name) || ep.Name == customConfig.Application.Name {
			continue
		}
		if stringInSlice(ep.Name[0:strings.Index(ep.Name, epPrefix)], workerNodesName) {
			EPsName = append(EPsName, ep.Name)
			klog.Infof("EPsName value = %s", ep.Name)
		}
	}
	for _, epName := range EPsName {
		realEP, _ := clientset.CoreV1().Endpoints("default").Get(epName, metav1.GetOptions{})
		annotation := realEP.ObjectMeta.Annotations
		if annotation == nil {
			klog.Infof("EP %s does not have traffic info annotations", epName)
			continue
		}
		annotation["reset"] = "true"
		realEP.ObjectMeta.Annotations = annotation
		_, error := clientset.CoreV1().Endpoints("default").Update(realEP)
		if error != nil {
			klog.Infof("Can not update old num requests for EP %s", realEP.Name)
		}
	}
}

func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

func isAllMapValueEqualToZero (inputMap map[string]int) bool {
	for _, v := range inputMap {
		if v != 0 {
			return false
		}
	}
	return true
}

func (c *safeIntValueMap) setIntValue(key string, value int) {
	// Lock so only one goroutine at a time can access the map c.intValueMap
	c.intValueMap[key] = value
}

func (c *safeIntValueMap) getInValue(key string) int {
	// Lock so only one goroutine at a time can access the map c.v.
	return c.intValueMap[key]
}

func (c *safeIntValueMap) getIntValueMap() map[string]int {
	return c.intValueMap
}

func (c *safeIntValueMap) printPodsDistributionInfo() {
	for k, v := range c.intValueMap {
		klog.Infof("Node %s has %d pods to schedule", k, v)
	}
}

func (c *safeFloatValueMap) setFloatValue(key string, value float64) {
	// Lock so only one goroutine at a time can access the map c.stringFloat64Map
	c.stringFloat64Map[key] = value
}

func (c *safeFloatValueMap) getValuesSum() float64 {
	sum := 0.0
	for _, v :=  range c.stringFloat64Map {
		sum += v
	}
	return sum
}

func (c *safeFloatValueMap) equalizeValuesTo1() {
	// Lock so only one goroutine at a time can access the map c.stringFloat64Map
	for k, _ :=  range c.stringFloat64Map {
		c.stringFloat64Map[k] = 1.0
	}
}

func (c *safeFloatValueMap) getFloat64Value(key string) float64 {
	// Lock so only one goroutine at a time can access the map c.v.
	return c.stringFloat64Map[key]
}

func (c *safeFloatValueMap) getFloat64ValueMap() map[string]float64 {
	return c.stringFloat64Map
}

func (c *safeFloatValueMap) printNodesTrafficInfo() {
	for k, v := range c.stringFloat64Map {
		klog.Infof("Node %s has traffic %f", k, v)
	}
}

func goid() int {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	idField := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))[0]
	id, err := strconv.Atoi(idField)
	if err != nil {
		panic(fmt.Sprintf("cannot get goroutine id: %v", err))
	}
	return id
}
