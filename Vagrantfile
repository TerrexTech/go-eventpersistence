# -*- mode: ruby -*-
# vi: set ft=ruby :

plugin_dependencies = [
  "vagrant-docker-compose",
  "vagrant-vbguest"
]

needsRestart = false

# Install plugins if required
plugin_dependencies.each do |plugin_name|
  unless Vagrant.has_plugin? plugin_name
    system("vagrant plugin install #{plugin_name}")
    needsRestart = true
    puts "#{plugin_name} installed"
  end
end

# Restart vagrant if new plugins were installed
if needsRestart === true
  exec "vagrant #{ARGV.join(' ')}"
end

Vagrant.configure(2) do |config|
  config.vm.define :persistencevm do |persistencevm|
    persistencevm.vm.hostname = "persistence"
    persistencevm.vm.box = "bento/ubuntu-16.04"

    persistencevm.vm.provider :virtualbox do |vb|
      vb.name = "persistence-vm"
      vb.gui = false
      vb.memory = "2024"
      vb.cpus = 2

      vb.customize ["modifyvm", :id, "--natdnshostresolver1", "on"]
    vb.customize ["modifyvm", :id, "--natdnsproxy1", "on"]
    end

  persistencevm.vm.network :forwarded_port,
        guest: 9092,
        host: 9092,
        auto_correct: true
  persistencevm.vm.network :forwarded_port,
        guest: 9042,
        host: 9042,
        auto_correct: true

    # Run as non-login shell, sourcing it to /etc/profile instead of /root/.profile
    # Due to clashing configurations for vagrant and base box.
    # See: https://github.com/mitchellh/vagrant/issues/1673#issuecomment-28288042
    persistencevm.ssh.shell = "bash -c 'BASH_ENV=/etc/profile exec bash'"

    persistencevm.vm.provision :docker
    persistencevm.vm.provision :docker_compose,
        compose_version: "1.22.0"

    # Automatically set current-dir to /vagrant on vagrant ssh
    persistencevm.vm.provision :shell,
        inline: "echo 'cd /vagrant' >> /home/vagrant/.bashrc"

    # Install some required packages
    # apt-update is automatically run during vagrant-vbguest install
    persistencevm.vm.provision :shell, inline: <<-SHELL
      apt-get install -y kafkacat
    SHELL
  end
end
