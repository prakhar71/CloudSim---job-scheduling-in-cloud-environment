/*
 * Title:        CloudSim Toolkit
 * Description:  CloudSim (Cloud Simulation) Toolkit for Modeling and Simulation
 *               of Clouds
 * Licence:      GPL - http://www.gnu.org/copyleft/gpl.html
 *
 * Copyright (c) 2009, The University of Melbourne, Australia
 */

package org.cloudbus.cloudsim.examples;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Stack;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.network.TopologicalGraph;
import org.cloudbus.cloudsim.network.TopologicalLink;
import org.cloudbus.cloudsim.network.TopologicalNode;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import org.w3c.dom.NodeList;

import sun.nio.cs.ext.TIS_620;

/**
 * A simple example showing how to create a datacenter with two hosts and run
 * two cloudlets on it. The cloudlets run in VMs with different MIPS
 * requirements. The cloudlets will take different time to complete the
 * execution depending on the requested VM performance.
 */
public class CloudSimTest1 {

	/** The cloudlet list. */
	private static List<Cloudlet> cloudletList;

	/** The vmlist. */
	private static List<Vm> vmlist;
	private static int cost[] = { 10, 50, 40, 60, 80, 70 };
	private static int[][] et = { { 3, 13, 17, 20, 6, 7 }, { 4, 9, 8, 18, 14, 15 }, { 6, 8, 4, 3, 12, 11 },
			{ 14, 23, 16, 17, 19, 1 }, { 2, 3, 17, 22, 12, 18 } };
	private static int[][] tt = { {}, {}, {}, {}, {} };

	private static TopologicalGraph graph = null;

	private static int deadline = 50;
	private static int acquisitiondelay = 1;
	private static int interval = 3;
	private static Stack<Integer> tentry_lst;
	private static Queue<Integer> to_be_scheduled;
	//private static int c = 0;

	/**
	 * Creates main() to run this example
	 */
	public static void main(String[] args) {

		Log.printLine("Starting CloudSimExample3...");

		try {
			// First step: Initialize the CloudSim package. It should be called
			// before creating any entities.
			int num_user = 1; // number of cloud users
			Calendar calendar = Calendar.getInstance();
			boolean trace_flag = false; // mean trace events

			// Initialize the CloudSim library
			CloudSim.init(num_user, calendar, trace_flag);

			// Second step: Create Datacenters
			// Datacenters are the resource providers in CloudSim. We need at list one of
			// them to run a CloudSim simulation
			Datacenter datacenter0 = createDatacenter("Datacenter_0");

			// Third step: Create Broker
			DatacenterBroker broker = createBroker();
			int brokerId = broker.getId();

			// Fourth step: Create one virtual machine
			vmlist = new ArrayList<Vm>();

			// VM description
			int vmid = 0;
			int mips = 250;
			long size = 10000; // image size (MB)
			int ram = 2048; // vm memory (MB)
			long bw = 1000;
			int pesNumber = 1; // number of cpus
			String vmm = "Xen"; // VMM name

			// create two VMs
			Vm vm1 = new Vm(vmid, brokerId, mips, pesNumber, ram, bw, size, vmm, new CloudletSchedulerTimeShared());

			// the second VM will have twice the priority of VM1 and so will receive twice
			// CPU time
			vmid++;
			Vm vm2 = new Vm(vmid, brokerId, mips * 2, pesNumber, ram + 1024, bw, size, vmm,
					new CloudletSchedulerTimeShared());

			vmid++;
			Vm vm3 = new Vm(vmid, brokerId, mips * 2, pesNumber, ram + 2048, bw, size, vmm,
					new CloudletSchedulerTimeShared());

			vmid++;
			Vm vm4 = new Vm(vmid, brokerId, mips * 2, pesNumber, ram, bw, size, vmm, new CloudletSchedulerTimeShared());

			vmid++;
			Vm vm5 = new Vm(vmid, brokerId, mips * 2, pesNumber, ram + 1024, bw, size, vmm,
					new CloudletSchedulerTimeShared());

			vmid++;
			Vm vm6 = new Vm(vmid, brokerId, mips * 2, pesNumber, ram + 2048, bw, size, vmm,
					new CloudletSchedulerTimeShared());

			// add the VMs to the vmList
			vmlist.add(vm1);
			vmlist.add(vm2);
			vmlist.add(vm3);
			vmlist.add(vm4);
			vmlist.add(vm5);
			vmlist.add(vm6);

			// submit vm list to the broker
			broker.submitVmList(vmlist);

			// Fifth step: Create two Cloudlets
			cloudletList = new ArrayList<Cloudlet>();

			// Cloudlet properties
			int id1 = 0;
			long length = 40000;
			long fileSize = 300;
			long outputSize = 300;
			UtilizationModel utilizationModel = new UtilizationModelFull();

			Cloudlet cloudlet1 = new Cloudlet(id1, length, pesNumber, fileSize, outputSize, utilizationModel,
					utilizationModel, utilizationModel);
			cloudlet1.setUserId(brokerId);

			int id2 = id1 + 1;
			length += 2000;
			fileSize += 20;
			outputSize += 50;

			Cloudlet cloudlet2 = new Cloudlet(id2, length, pesNumber, fileSize, outputSize, utilizationModel,
					utilizationModel, utilizationModel);
			cloudlet2.setUserId(brokerId);

			int id3 = id2 + 1;
			length += 1000;
			fileSize += 30;
			outputSize += 30;

			Cloudlet cloudlet3 = new Cloudlet(id3, length, pesNumber, fileSize, outputSize, utilizationModel,
					utilizationModel, utilizationModel);
			cloudlet3.setUserId(brokerId);

			int id4 = id3 + 1;
			length += 4000;
			fileSize += 60;
			outputSize += 50;

			Cloudlet cloudlet4 = new Cloudlet(id4, length, pesNumber, fileSize, outputSize, utilizationModel,
					utilizationModel, utilizationModel);
			cloudlet4.setUserId(brokerId);

			int id5 = id4 + 1;
			length += 3000;
			fileSize += 70;
			outputSize += 50;

			Cloudlet cloudlet5 = new Cloudlet(id5, length, pesNumber, fileSize, outputSize, utilizationModel,
					utilizationModel, utilizationModel);
			cloudlet5.setUserId(brokerId);

			// add the cloudlets to the list
			cloudletList.add(cloudlet1);
			cloudletList.add(cloudlet2);
			cloudletList.add(cloudlet3);
			cloudletList.add(cloudlet4);
			cloudletList.add(cloudlet5);

			// submit cloudlet list to the broker
			broker.submitCloudletList(cloudletList);

			if (graph == null) {
				graph = new TopologicalGraph();
				TopologicalNode node1 = new TopologicalNode(id1);
				TopologicalNode node2 = new TopologicalNode(id2);
				TopologicalNode node3 = new TopologicalNode(id3);
				TopologicalNode node4 = new TopologicalNode(id4);
				TopologicalNode node5 = new TopologicalNode(id5);

				TopologicalLink l12 = new TopologicalLink(id1, id2, 12, 9000);
				TopologicalLink l13 = new TopologicalLink(id1, id3, 15, 2000);
				TopologicalLink l34 = new TopologicalLink(id3, id4, 17, 3000);
				TopologicalLink l25 = new TopologicalLink(id2, id5, 14, 2000);
				TopologicalLink l45 = new TopologicalLink(id4, id5, 10, 1000);

				graph.addNode(node1);
				graph.addNode(node2);
				graph.addNode(node3);
				graph.addNode(node4);
				graph.addNode(node5);

				graph.addLink(l12);
				graph.addLink(l13);
				graph.addLink(l34);
				graph.addLink(l25);
				graph.addLink(l45);

				// initialise root node list
				tentry_lst = new Stack<Integer>();
				tentry_lst.add(0);
				// initialise Vm pool status and sheduler class;
				VMPoolStatus pool = new VMPoolStatus();
				Schedule sch = new Schedule();
				to_be_scheduled = new LinkedList<>();

				int max = Integer.MIN_VALUE;

				for (int t = 0; t < 5; t++) {
					int mst_ti = MET(et, t);
					int est_ti = EST(et, t, tt);
					int eft_ti = mst_ti + est_ti;

					if (eft_ti > max) {
						max = eft_ti;
					}

				}
				int MET_W = max;

				if (deadline > MET_W) {

					pre_processing(graph);
					// compute MET,LFT,XET matrix
					Iterator<Integer> itr = tentry_lst.iterator();
					while (itr.hasNext()) {
						Vm To_Provision = cheapestTaskVMmap(itr.next());
						// schedule
						sch.task = itr.next();
						sch.VMid = To_Provision.getId();
						sch.xst = acquisitiondelay;
						sch.xft = sch.xst + tt[itr.next()][itr.next()] + et[itr.next()][To_Provision.getId()];

						// update Vm pool status
						pool.VMid = To_Provision.getId();
						pool.VM_type = To_Provision;
						pool.xist = sch.xst + et[itr.next()][To_Provision.getId()];

					}
					to_be_scheduled.add(cloudletList.get(0).getUserId());//add t1 to the to_be_scheduled for starting
					while (!to_be_scheduled.isEmpty()) {

						Iterator<TopologicalNode> itr1 = children(to_be_scheduled.remove()).iterator();
						while (itr1.hasNext()) {
							to_be_scheduled.add(itr1.next().nodeID);
						}// removing t1 adding the children of t1 in the 1st iteration to to_be_scheduled
						// check if the task in to_be_scheduled r sch or not
						chklistsch(to_be_scheduled);
						//sch the tasks in to_be_scheduled
						planandschedule(to_be_scheduled);

					}

				} else {
					Log.printLine("Deadline is too less ....");
				}

			}

			// bind the cloudlets to the vms. This way, the broker
			// will submit the bound cloudlets only to the specific VM
			/*
			 * broker.bindCloudletToVm(cloudlet1.getCloudletId(),vm1.getId());
			 * broker.bindCloudletToVm(cloudlet2.getCloudletId(),vm2.getId());
			 * broker.bindCloudletToVm(cloudlet3.getCloudletId(),vm3.getId());
			 * broker.bindCloudletToVm(cloudlet4.getCloudletId(),vm4.getId());
			 *
			 * 
			 */

			// Sixth step: Starts the simulation
			CloudSim.startSimulation();

			// Final step: Print results when simulation is over
			List<Cloudlet> newList = broker.getCloudletReceivedList();

			CloudSim.stopSimulation();

			printCloudletList(newList);

			// Print the debt of each user to each datacenter
			datacenter0.printDebts();

			Log.printLine("CloudSimExample3 finished!");
		} catch (Exception e) {
			e.printStackTrace();
			Log.printLine("The simulation has been terminated due to an unexpected error");
		}
	}

	private static void chklistsch(Queue<Integer> to_be_scheduled1) {
		// TODO Auto-generated method stub

		int c=0;// init the counter
		
		while (c < to_be_scheduled1.size()) {
			if (!isScheduled(to_be_scheduled1.peek())) {//chk if task is not sch we 
				//dont hve to do nything
				c++;

			} else {// but if tsk is not sch
				int x = to_be_scheduled1.remove();
				List<TopologicalNode> childlst = children(x);//extracting the childrens
				Iterator<TopologicalNode> itr = childlst.iterator();
				while (itr.hasNext()) {
					if (to_be_scheduled1.contains(itr.next())) {// chk if to_be_scheduled already contains the extracted children
							c++;
					} else {// if child is new then
						to_be_scheduled1.add(itr.next().nodeID);
						
						chklistsch(to_be_scheduled1);//Recursion
						c++;
					}
				}

			}
		}
		

	}
	
	private static void chklistsch1() {
		int c= 0;
		int l = to_be_scheduled.size();
		while(c <l) {
			if(!isScheduled(to_be_scheduled.peek())) {
				
			}else {
				to_be_scheduled.remove();
				
			}
			
			c++;
		}
	}

	private static boolean isScheduled(int t) {
		// TODO Auto-generated method stub
		
		
		return false;
	}

	private static void planandschedule(Queue<Integer> to_be_scheduled2) {
		// TODO Auto-generated method stub

	}

	private static Vm cheapestTaskVMmap(int t) {
		int xst_ti;
		Vm taskvmmap = null;
		if (t != 0) {

			List<TopologicalNode> tp = parentlst(t);
			Iterator<TopologicalNode> itr = tp.iterator();
			int max = Integer.MIN_VALUE;
			while (itr.hasNext()) {
				int temp = XFT(itr.next().nodeID, Vk, tt, et);
			}

		} else {

			xst_ti = XST(t, tt);
		}

		Iterator<Vm> itr = vmlist.iterator();
		List<Vm> VMk;
		while (itr.hasNext()) {
			int var = xst_ti + XET(et, t, itr.next().getId());
			if (var <= deadline) {
				VMk.add(itr.next());
			}
		}

		Iterator<Vm> itr1 = VMk.iterator();
		int min = Integer.MAX_VALUE;
		Vm min_node = null;
		while (itr1.hasNext()) {
			int var1 = XET(et, t, itr1.next().getId()) / interval * cost[itr1.next().getId()];
			if (var1 < min) {
				min = var1;
				min_node = itr1.next();
			}
		}

		taskvmmap = min_node;
		return taskvmmap;
	}

	private static int XST(int t, int[][] tt) {

		if (t == 0) {
			return acquisitiondelay;
		} else {

			return t;
		}
	}

	private static int XFT(int t, int Vk, int[][] tt, int[][] et) {

	}

	private static boolean isExecuting(int t) {
		// TODO Auto-generated method stub
		return false;
	}

	private static int MET(int[][] et, int t) {
		int min = Integer.MAX_VALUE;
		for (int i = 0; i < et[0].length; i++) {
			if (et[t][i] < min) {
				min = et[t][i];
			}
		}
		return min;
	}

	private static int EST(int[][] et, int t, int[][] tt) {
		if (t == 0) {
			return 0;
		} else if (t == 2) {
			int var = EST(et, t - 2, tt) + MET(et, t - 2) + tt[t - 2][t];
			return var;
		} else if (t == 4) {
			int var1 = EST(et, t - 1, tt) + MET(et, t - 1) + tt[t - 1][t];
			int var2 = EST(et, t - 2, tt) + MET(et, t - 2) + tt[t - 2][t];

			return Math.max(var1, var2);

		} else {
			int var = EST(et, t - 1, tt) + MET(et, t - 1) + tt[t - 1][t];
			return var;
		}
	}

	private static int LFT(int[][] et, int t, int[][] tt) {
		int texit = graph.nodeList.get(4).nodeID;
		if (t == texit) {
			return deadline;
		} else {
			List<TopologicalNode> templst = children(t);
			Iterator<TopologicalNode> itr = templst.iterator();
			int min = Integer.MAX_VALUE;
			while (itr.hasNext()) {

				int exp = LFT(et, itr.next().nodeID, tt) - MET(et, itr.next().nodeID) - tt[t][itr.next().nodeID];
				if (exp < min) {
					min = exp;
				}
			}
			return min;
		}

	}

	private static int XET(int[][] et, int t, int Vmv) {
		int texit = graph.nodeList.get(4).nodeID;
		if (t == texit) {
			return et[t][Vmv];
		} else {
			List<TopologicalNode> templst = children(t);
			Iterator<TopologicalNode> itr = templst.iterator();
			int mx = Integer.MIN_VALUE;
			while (itr.hasNext()) {
				int exp = XET(et, itr.next().nodeID, Vmv);
				if (exp > mx) {
					mx = exp;
				}
			}
			int exp1 = et[t][Vmv] + mx;
			return exp1;
		}

	}

	private static void pre_processing(TopologicalGraph graph) {

		Queue<TopologicalNode> tksqueue = new LinkedList<>();
		List<TopologicalNode> nodelst = graph.nodeList;
		tksqueue.add(nodelst.get(0));

		while (!tksqueue.isEmpty()) {
			TopologicalNode tp = tksqueue.remove();
			List<TopologicalNode> Sc = children(tp);
			TopologicalNode tc = Sc.get(0);
			if (Sc.size() == 1 && isparent(tc)) {
				Cloudlet cp = getCloudlet(tp);
				Cloudlet cc = getCloudlet(tc);
				int new_id = cp.getUserId() + cc.getUserId();
				long new_length = (cp.getCloudletLength() + cc.getCloudletLength());
				int new_pes = cp.getNumberOfPes() + cc.getNumberOfPes();
				long new_file_size = cp.getCloudletFileSize() + cc.getCloudletFileSize();
				long new_cloudletOutputSize = cp.getCloudletOutputSize() + cc.getCloudletOutputSize();
				UtilizationModel utilizationModelCpu = cp.getUtilizationModelCpu();
				UtilizationModel utilizationModelRam = cp.getUtilizationModelRam();
				UtilizationModel utilizationModelBw = cp.getUtilizationModelBw();
				Cloudlet new_cloudlet = new Cloudlet(new_id, new_length, new_pes, new_file_size, new_cloudletOutputSize,
						utilizationModelCpu, utilizationModelRam, utilizationModelBw);
				// remove cloudlet tp and tc
				cloudletList.remove(cp);

				cloudletList.set(cc.getUserId(), new_cloudlet);

				Iterator<TopologicalNode> itr = graph.nodeList.iterator();
				while (itr.hasNext()) {
					if (itr.next().nodeID == tp.nodeID || itr.next().nodeID == tc.nodeID) {
						itr.remove();
					}
				}

				Iterator<TopologicalLink> itr1 = graph.linkList.iterator();

				while (itr1.hasNext()) {
					if (itr1.next().getSrcNodeID() == tp.nodeID || itr1.next().getDestNodeID() == tc.nodeID) {

						itr1.remove();
					}
				}

				// set tp+c as parent of tc
				TopologicalNode new_node = new TopologicalNode(new_id);
				TopologicalLink l2n = new TopologicalLink(1, new_id, 0, 0);
				TopologicalLink ln5 = new TopologicalLink(new_id, 4, 0, 0);

				// update ET
				int[][] temp = { { 3, 13, 17, 20, 6, 7 }, { 4, 9, 8, 18, 14, 15 }, { 20, 31, 20, 20, 31, 12 },
						{ 2, 3, 17, 22, 12, 18 } };

				et = temp;

				// add tp+c to front
				tksqueue.add(new_node);

			} else {
				Iterator<TopologicalNode> itr = Sc.iterator();
				while (itr.hasNext()) {
					tksqueue.add(itr.next());
				}
			}

		}
	}

	private static Cloudlet getCloudlet(TopologicalNode node) {
		// TODO Auto-generated method stub
		Iterator<Cloudlet> itr = cloudletList.iterator();
		while (itr.hasNext()) {
			Cloudlet c = itr.next();
			if (c.getUserId() == node.nodeID) {
				return c;
			}
		}

		return null;
	}

	private static boolean isparent(TopologicalNode topologicalNode) {
		// TODO Auto-generated method stub
		return false;
	}

	private static List<TopologicalNode> parentlst(int t) {

		return null;
	}

	private static List<TopologicalNode> children(TopologicalNode node) {

		return null;
	}

	private static List<TopologicalNode> children(int t) {
		return null;
	}

	private static Datacenter createDatacenter(String name) {

		// Here are the steps needed to create a PowerDatacenter:
		// 1. We need to create a list to store
		// our machine
		List<Host> hostList = new ArrayList<Host>();

		// 2. A Machine contains one or more PEs or CPUs/Cores.
		// In this example, it will have only one core.
		List<Pe> peList = new ArrayList<Pe>();

		int mips = 1000;

		// 3. Create PEs and add these into a list.
		peList.add(new Pe(0, new PeProvisionerSimple(mips))); // need to store Pe id and MIPS Rating

		// 4. Create Hosts with its id and list of PEs and add them to the list of
		// machines
		int hostId = 0;
		int ram = 2048; // host memory (MB)
		long storage = 1000000; // host storage
		int bw = 10000;

		hostList.add(new Host(hostId, new RamProvisionerSimple(ram), new BwProvisionerSimple(bw), storage, peList,
				new VmSchedulerTimeShared(peList))); // This is our first machine

		// create another machine in the Data center
		List<Pe> peList2 = new ArrayList<Pe>();

		peList2.add(new Pe(0, new PeProvisionerSimple(mips)));

		hostId++;

		hostList.add(new Host(hostId, new RamProvisionerSimple(ram), new BwProvisionerSimple(bw), storage, peList2,
				new VmSchedulerTimeShared(peList2))); // This is our second machine

		// 5. Create a DatacenterCharacteristics object that stores the
		// properties of a data center: architecture, OS, list of
		// Machines, allocation policy: time- or space-shared, time zone
		// and its price (G$/Pe time unit).
		String arch = "x86"; // system architecture
		String os = "Linux"; // operating system
		String vmm = "Xen";
		double time_zone = 10.0; // time zone this resource located
		double cost = 3.0; // the cost of using processing in this resource
		double costPerMem = 0.05; // the cost of using memory in this resource
		double costPerStorage = 0.001; // the cost of using storage in this resource
		double costPerBw = 0.0; // the cost of using bw in this resource
		LinkedList<Storage> storageList = new LinkedList<Storage>(); // we are not adding SAN devices by now

		DatacenterCharacteristics characteristics = new DatacenterCharacteristics(arch, os, vmm, hostList, time_zone,
				cost, costPerMem, costPerStorage, costPerBw);

		// 6. Finally, we need to create a PowerDatacenter object.
		Datacenter datacenter = null;
		try {
			datacenter = new Datacenter(name, characteristics, new VmAllocationPolicySimple(hostList), storageList, 0);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return datacenter;
	}

	// We strongly encourage users to develop their own broker policies, to submit
	// vms and cloudlets according
	// to the specific rules of the simulated scenario
	private static DatacenterBroker createBroker() {

		DatacenterBroker broker = null;
		try {
			broker = new DatacenterBroker("Broker");
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return broker;
	}

	/**
	 * Prints the Cloudlet objects
	 * 
	 * @param list list of Cloudlets
	 */
	private static void printCloudletList(List<Cloudlet> list) {
		int size = list.size();
		Cloudlet cloudlet;

		String indent = "    ";
		Log.printLine();
		Log.printLine("========== OUTPUT ==========");
		Log.printLine("Cloudlet ID" + indent + "STATUS" + indent + "Data center ID" + indent + "VM ID" + indent + "Time"
				+ indent + "Start Time" + indent + "Finish Time");

		DecimalFormat dft = new DecimalFormat("###.##");
		for (int i = 0; i < size; i++) {
			cloudlet = list.get(i);
			Log.print(indent + cloudlet.getCloudletId() + indent + indent);

			if (cloudlet.getCloudletStatus() == Cloudlet.SUCCESS) {
				Log.print("SUCCESS");

				Log.printLine(indent + indent + cloudlet.getResourceId() + indent + indent + indent + cloudlet.getVmId()
						+ indent + indent + dft.format(cloudlet.getActualCPUTime()) + indent + indent
						+ dft.format(cloudlet.getExecStartTime()) + indent + indent
						+ dft.format(cloudlet.getFinishTime()));
			}
		}

	}
}

class Schedule {

	protected int task;
	protected int VMid;
	protected int xst;
	protected int xft;
}

class VMPoolStatus {
	protected int VMid;
	protected Vm VM_type;
	protected int ast;
	protected int xist;
	protected int end_time;
}
