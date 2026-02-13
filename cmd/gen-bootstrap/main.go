package main

import (
	"context"
	"fmt"
	"os"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/butlerdotdev/butler-controller/internal/talos"
)

func main() {
	home, _ := os.UserHomeDir()
	mgmtConfig, err := clientcmd.BuildConfigFromFlags("", home+"/.butler/butler-beta-kubeconfig")
	if err != nil {
		panic(fmt.Sprintf("mgmt kubeconfig: %v", err))
	}
	mgmtClient, err := kubernetes.NewForConfig(mgmtConfig)
	if err != nil {
		panic(fmt.Sprintf("mgmt client: %v", err))
	}

	ctx := context.Background()
	ns := "talos-e2e-8819ab5e"
	tcName := "talos-e2e"

	caSecret, err := mgmtClient.CoreV1().Secrets(ns).Get(ctx, tcName+"-ca", metav1.GetOptions{})
	if err != nil {
		panic(fmt.Sprintf("CA secret: %v", err))
	}
	clusterCACert := string(caSecret.Data["ca.crt"])
	fmt.Printf("Got cluster CA (%d bytes)\n", len(clusterCACert))

	trustdSecret, err := mgmtClient.CoreV1().Secrets(ns).Get(ctx, tcName+"-trustd-creds", metav1.GetOptions{})
	if err != nil {
		panic(fmt.Sprintf("trustd creds: %v", err))
	}
	machineToken := string(trustdSecret.Data["token"])
	osCACert := string(trustdSecret.Data["os-ca.crt"])
	fmt.Printf("Got machine token (%d bytes), OS CA (%d bytes)\n", len(machineToken), len(osCACert))

	tenantKubeconfigSecret, err := mgmtClient.CoreV1().Secrets(ns).Get(ctx, tcName+"-admin-kubeconfig", metav1.GetOptions{})
	if err != nil {
		panic(fmt.Sprintf("tenant kubeconfig: %v", err))
	}
	tenantConfig, err := clientcmd.RESTConfigFromKubeConfig(tenantKubeconfigSecret.Data["admin.conf"])
	if err != nil {
		panic(fmt.Sprintf("parse tenant kubeconfig: %v", err))
	}
	tenantClient, err := kubernetes.NewForConfig(tenantConfig)
	if err != nil {
		panic(fmt.Sprintf("tenant client: %v", err))
	}

	bootstrapToken, err := talos.FindExistingBootstrapToken(ctx, tenantClient)
	if err != nil {
		panic(fmt.Sprintf("find bootstrap token: %v", err))
	}
	if bootstrapToken != "" {
		fmt.Printf("Reusing existing bootstrap token: %s...\n", bootstrapToken[:6])
	} else {
		bootstrapToken, err = talos.GenerateBootstrapToken()
		if err != nil {
			panic(fmt.Sprintf("generate token: %v", err))
		}
		if err := talos.CreateBootstrapTokenSecret(ctx, tenantClient, bootstrapToken); err != nil {
			panic(fmt.Sprintf("create token: %v", err))
		}
		fmt.Printf("Created bootstrap token: %s...\n", bootstrapToken[:6])
	}

	input := talos.MachineConfigInput{
		ClusterName:          tcName,
		ControlPlaneEndpoint: "https://10.40.0.215:6443",
		ClusterCACert:        clusterCACert,
		MachineToken:         machineToken,
		BootstrapToken:       bootstrapToken,
		OSCACert:             osCACert,
		PodCIDR:              "10.244.0.0/16",
		ServiceCIDR:          "10.96.0.0/12",
		InstallDisk:          "/dev/vda",
		InstallerImage:       "factory.talos.dev/installer/ce4c980550dd2ab1b17bbf2b08801c7eb59418eafe8f279833297925d67c7515:v1.9.3",
	}

	machineConfigData, err := talos.GenerateWorkerConfig(input)
	if err != nil {
		panic(fmt.Sprintf("generate config: %v", err))
	}
	fmt.Printf("Generated machine config (%d bytes)\n", len(machineConfigData))
	os.WriteFile("/tmp/talos-e2e-machineconfig.yaml", machineConfigData, 0644)
	fmt.Println("Wrote machine config to /tmp/talos-e2e-machineconfig.yaml")

	bootstrapSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      tcName + "-talos-bootstrap",
			Namespace: ns,
			Labels: map[string]string{
				"app.kubernetes.io/managed-by": "butler",
				"butler.butlerlabs.dev/tenant": tcName,
			},
		},
		Data: map[string][]byte{
			"value": machineConfigData,
		},
	}

	_, err = mgmtClient.CoreV1().Secrets(ns).Create(ctx, bootstrapSecret, metav1.CreateOptions{})
	if err != nil {
		panic(fmt.Sprintf("create bootstrap secret: %v", err))
	}
	fmt.Println("Created bootstrap secret: " + tcName + "-talos-bootstrap")
}
